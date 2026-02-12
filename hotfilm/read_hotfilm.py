"""
ReadHotfilm class for reading raw NIDAS hotfilm voltage data files into
Datasets.
"""
import re
import datetime as dt
import subprocess as sp
import time
import logging

from typing import Generator, Tuple, Union
from typing import Optional, List

import numpy as np
import pandas as pd
import xarray as xr

from hotfilm.outout_path import OutputPath
from hotfilm.utils import combine_datasets
from hotfilm.utils import convert_time_coordinate
from hotfilm.utils import td_to_microseconds
from hotfilm.utils import split_dataset
from hotfilm.utils import add_history_to_dataset
from hotfilm.utils import rdatetime
from hotfilm.time_formatter import time_formatter


logger = logging.getLogger(__name__)


def _ft(dt64):
    return np.datetime_as_string(dt64, unit='us')


# this is the data_dump timestamp prefix that needs to be matched. times are
# explicitly in iso format with microsecond precision, no deltat and no len
# fields, since they are not needed.
#
# 2023-09-20T18:15:42.843250 200, 521  1 2 3 4

_prefix_rx = re.compile(
    r"^(?P<year>\d{4})-(?P<month>\d{2})-(?P<day>\d{2})T"
    r"(?P<hour>\d{2}):(?P<minute>\d{2}):(?P<second>\d{2}\.?\d*) *"
    r"(?P<dsmid>\d+), *(?P<spsid>\d+) (?P<data>.*)$")


def _datetime_from_match(match) -> np.datetime64:
    # split seconds at the decimal to get microseconds
    seconds, _, usecs = match['second'].partition('.')
    seconds = int(seconds)
    usecs = int((usecs + '000000')[:6]) if usecs else 0
    when = dt.datetime(int(match['year']), int(match['month']),
                       int(match['day']),
                       int(match['hour']), int(match['minute']),
                       seconds, usecs)
    when = np.datetime64(when, 'ns')
    return when


class HotfilmDataNotice:
    """
    Encapsulate anomalies and discrepancies in the data processing, such as
    warnings or corrections, so they can be logged, reported at the end, and
    also included in the data as needed.  A notice can refer to the sample
    time of a specific output scan.  If the scan time was corrected, then the
    notice contains the corrected time so it can be lined up with the data in
    the output.  The goal is to capture structured information for different
    kinds of corrections, warnings, and notices, so the notices can be
    reported in the output in a form that could later be parsed for
    information, while also writing the notice to the log.  Notices without
    times can still be ordered relative to other notices.
    """

    # the time of the last scan when a jump ended
    _jump_end: np.datetime64

    def __init__(self, scan: xr.Dataset = None):
        """
        @p scan is the scan related to this notice.  It should already have a
        time dimension, so the first time is used as the time of the notice.
        """
        self._message = None
        self._scantime = scan.time[0].data if scan is not None else None
        self._ncorrected = 0
        self._nfilled = 0
        self._nskipped = 0
        self._nwarnings = 0
        self._njumps = 0
        self._jump_end = None
        self._fill_ranges = None

    def scantime(self, when: np.datetime64):
        self._scantime = when
        return self

    def time_corrected_from(self, old_time: np.datetime64):
        self._message = "scan time fixed, from %s to %s" % (
            _ft(old_time), _ft(self._scantime))
        logger.info(self._message)
        self._ncorrected += 1
        return self

    def time_jump_fixed(self, time0: np.datetime64, timep: np.datetime64):
        "Accumulate this jump into this notice or start a new jump."
        # a time jump notice keeps updating the jump range but does not change
        # _scantime, since that records the first scan in the jump and
        # determines where the notice is included with the output.
        self._jump_end = timep
        self._ncorrected += 1
        self._njumps += 1
        self._message = (f"fix scan time {_ft(time0)} to "
                         f"{_ft(timep)}, {self._njumps} jumps "
                         f"since {_ft(self._scantime)}")
        logger.info(self._message)
        return self

    def filled_values(self, v: xr.DataArray, nvalues: int,
                      fill_ranges: List[tuple[int, int]]):
        self._nfilled += nvalues
        self._fill_ranges = fill_ranges
        self._message = (
            f"scan {_ft(self._scantime)}, variable {v.name}[0, {len(v)-1}], "
            f"filled {nvalues} nans at indices: {fill_ranges}")
        logger.info(self._message)
        return self

    def warning(self, message: str):
        self._message = message
        logger.warning(message)
        self._nwarnings += 1
        return self

    def notice(self, message: str):
        "Set a message that is not necessarily a warning, logged as info."
        self._message = message
        logger.info(message)
        return self

    def to_string(self) -> str:
        """
        Return a string representation which tries to preserve the structured
        information. Perhaps JSON could be used instead, but this is more
        human readable and hopefully still parseable.
        """
        buffer = ""
        if self._scantime is not None:
            buffer += f"scantime={_ft(self._scantime)}; "
        if self._ncorrected:
            buffer += f"ncorrected={self._ncorrected}; "
        if self._nfilled:
            buffer += f"nfilled={self._nfilled}; "
        if self._nwarnings:
            buffer += f"nwarnings={self._nwarnings}; "
        if self._fill_ranges:
            buffer += f"fill_ranges={self._fill_ranges}; "
        if self._njumps:
            buffer += f"njumps={self._njumps}; "
        if self._jump_end is not None:
            buffer += f"jump_end={_ft(self._jump_end)}; "
        if self._message:
            buffer += f"message={self._message}; "
        return buffer.strip()


class ReadHotfilm:
    """
    Read the hotfilm 1-second time series from data_dump.
    """

    cmd: List[str]
    adjust_time: int
    scan: Optional[xr.Dataset]
    next_scan: Optional[xr.Dataset]
    command_line: str
    begin: Optional[np.datetime64]
    end: Optional[np.datetime64]
    keep_contiguous: bool
    minblock: np.timedelta64
    maxblock: np.timedelta64
    file_interval: np.timedelta64
    notices: List[HotfilmDataNotice]
    all_notices: List[HotfilmDataNotice]

    # really these should come from the xml, but hardcode for now
    HEIGHTS = {
        'ch0': '0.5m',
        'ch1': '1m',
        'ch2': '2m',
        'ch3': '4m'
    }
    SITE = 't0'
    ALL_CHANNELS = ['ch0', 'ch1', 'ch2', 'ch3']
    CHANNEL_IDS = {
        'ch0': 520,
        'ch1': 521,
        'ch2': 522,
        'ch3': 523
    }
    ADC_STATUS_ID = 501
    SCAN_DIM = 'time_scan_start'

    def __init__(self):
        self.source = ["sock:192.168.1.220:31000"]
        self.cmd = []
        self.dd = None
        # default to all channels, otherwise a list of channel names
        self.channels = self.ALL_CHANNELS
        # insert a delay between samples read from a file
        self.delay = 0
        # Dataset for the current scan as it accumulates channels
        self.scan = None
        # cache the start of the next block
        self.next_scan = None
        # limit output to inside the begin and end times, if set
        self.begin = None
        self.end = None
        self.timeformat = time_formatter.FLOAT_SECONDS
        # minimum duration required to consider a block good
        self.minblock = np.timedelta64(0, 'm')
        # maximum duration to include in a block
        self.maxblock = np.timedelta64(0, 'm')
        # interval at which to start new files
        self.file_interval = np.timedelta64(60, 'm')
        # adjustment to successive sample times to line them up with previous
        # samples as the labjack clock drifts relative to the system time.
        self.adjust_time = 0
        # iterator which returns the next data line
        self.line_iterator = None
        self.precision = 8
        self.command_line = ""
        # keep track of the sample rate in case it changes
        self.sample_rate = 0
        # if true, adjust sample times in contiguous blocks to keep them
        # exactly at the nominal sample rate, even when the labjack clock
        # drifts relative to GPS.
        self.keep_contiguous = False
        # keep two notices lists, one for all notices and one for the next
        # dataset to write out.  this way there is a summary of all notices
        # across all datasets to report at the end.
        self.notices = []
        self.all_notices = []

    def get_notices(self) -> List[HotfilmDataNotice]:
        return self.all_notices

    def num_notices(self):
        return len(self.all_notices)

    def num_warnings(self):
        return sum([n._nwarnings for n in self.all_notices])

    def num_corrected(self):
        return sum([n._ncorrected for n in self.all_notices])

    def notice(self, scan: xr.Dataset = None) -> HotfilmDataNotice:
        """
        Create a notice related to the given scan and return it.
        """
        hdn = HotfilmDataNotice(scan)
        self.notices.append(hdn)
        self.all_notices.append(hdn)
        return hdn

    def clear_notices(self):
        "Clear the current notices.  Useful after writing out with a Dataset."
        self.notices.clear()

    def time_jump_fixed(self, time0: np.datetime64, timep: np.datetime64):
        """
        Create or update a notice for one or more scans fixed for time jumps.
        """
        # if this is a new jump notice, create it, otherwise update it.
        notice = self.notices[-1] if self.notices else None
        if not notice or not notice._njumps:
            notice = self.notice().scantime(timep)
        notice.time_jump_fixed(time0, timep)
        return notice

    def set_command_line(self, argv: List[str]):
        self.command_line = " ".join([f"'{arg}'" if ' ' in arg else arg
                                      for arg in argv])

    def set_time_format(self, fspec):
        """
        Set the time format specifier to @p fspec.  Passing None sets it to
        the default.
        """
        if not fspec:
            self.timeformat = time_formatter.FLOAT_SECONDS
        else:
            self.timeformat = fspec

    def set_min_max_block_minutes(self, mmin: int, mmax: int):
        self.minblock = np.timedelta64(mmin, 'm')
        self.maxblock = np.timedelta64(mmax, 'm')

    def format_time(self, when: np.datetime64):
        "Convenient shortcut, but not optimal."
        return time_formatter(self.timeformat, when)(when)

    def set_source(self, source):
        logger.info("setting sources: %s", ",".join(source))
        self.source = source

    def _make_cmd(self):
        # this requires NIDAS 1.2.6
        self.cmd = ["data_dump", "--precision", str(self.precision),
                    "--nodeltat", "--nolen",
                    "--timeformat", "%Y-%m-%dT%H:%M:%S.%6f"]
        self.cmd += ["-i", f"/,{self.ADC_STATUS_ID}"]
        # add samples for each channel
        for ch in self.channels:
            self.cmd += ["-i", f"/,{self.CHANNEL_IDS[ch]}"]
        self.cmd += self.source

    def start(self):
        self._make_cmd()
        command = " ".join(self.cmd)
        logger.info("running: %s%s", command[:60],
                    "..." if command[60:] else "")
        logger.debug("full command: %s", command)
        self.dd = sp.Popen(self.cmd, stdout=sp.PIPE, text=True)
        self.line_iterator = self.dd.stdout

    def select_channels(self, channels: Union[list[int], None]):
        self.channels = [f"ch{ch}" for ch in channels or []
                         if f"ch{ch}" in self.ALL_CHANNELS]
        if not self.channels:
            self.channels = self.ALL_CHANNELS
        logger.debug("selected channels: %s",
                     ",".join(self.channels) if self.channels else "all")

    def get_data(self, scan: xr.Dataset = None) -> Union[xr.DataArray, None]:
        """
        Return the next selected channel as a DataArray.
        """
        if not self.line_iterator:
            return None
        data = None
        while data is None:
            if not (line := next(self.line_iterator, None)):
                break
            data = self.parse_line(line, scan)

        if data is not None and self.delay:
            time.sleep(self.delay)
        return data

    def skip_scan(self, scan: xr.Dataset) -> bool:
        """
        Return True if any data variables (no sense checking time) in this
        scan contain dummy values.  @p scan must be a Dataset with one or more
        channels.
        """
        skip = any([(x == -9999.0).any() for x in scan.data_vars.values()])
        return skip

    def fill_scan(self, scan: xr.Dataset) -> xr.Dataset:
        """
        Replace any dummy values in this scan with nans, and create a notice
        about the filled values.
        """
        fill_ranges = []
        nvalues = 0
        for v in scan.data_vars.values():
            indices = np.where(v == -9999.0)[0]
            if indices.size == 0:
                continue
            v[indices] = np.nan
            # the fill ranges are all the same, so compute and log them once
            if fill_ranges:
                continue
            nvalues = len(indices)
            begin, end = None, None
            for i in indices:
                if begin is None:
                    begin, end = i, i
                elif i == end + 1:
                    end = i
                else:
                    fill_ranges.append((int(begin), int(end)))
                    begin, end = None, None
            if begin is not None:
                fill_ranges.append((int(begin), int(end)))
            self.notice(scan).filled_values(v, nvalues, fill_ranges)

        return scan

    def get_scan(self) -> Optional[xr.Dataset]:
        """
        Return a Dataset with all the channels in a single scan.  A scan is
        all channels with the same timestamp and the same sample rate.
        """
        # The full scan to be returned.
        scan = self.scan
        while True:
            data = self.get_data(scan)
            if data is None:
                # return current scan, no pending scan
                self.scan = None
                break
            if scan is None:
                scan = data
            if data is not scan:
                # started a new scan. the current scan, if any, is what will
                # be returned.  when this method is called again, the pending
                # scan will be filled with the succeeding samples.
                self.scan = data
                break

        return scan

    def is_contiguous(self, ds: xr.Dataset, scan: xr.Dataset) -> bool:
        """
        Return true if @p scan looks contiguous with @p ds, and if so, adjust
        the timestamps in @p scan accordingly.

        The next 1-second scan is contiguous if it starts within 1 second and
        two sample periods relative to the expected start.  This allows shifts
        of whole seconds, where the wrong system second was used as the
        reference for the PPS, since that might not be handled during
        acquisition.  This just allows for the PPS index to drift over time
        because the labjack clock is not synchronized to an absolute
        reference.  As the PPS index shifts, then the sample time will shift
        by the scan interval.  So the generated sample times drift relative to
        actual absolute time, but the generated times will have the expected
        regular interval matching the scan rate.  If the adjustment gets too
        large, flag the next frame as not contigous so the next sample time
        resets to the absolute time when it was recorded.

        This also assumes that the very first scan in a block was aligned to
        the correct second of absolute time, since all following scans will be
        aligned to it.  I think in the worse case a block could be off by a
        second.
        """
        next = scan.time[0] + np.timedelta64(self.adjust_time, 'us')
        interval = self.get_interval(ds)
        interval_usecs = interval.astype(int)
        # the expected start of the next scan is last + interval, and the
        # shift between expected time and actual time is calculated with the
        # current time adjustment included.  the shift is how much to add to
        # the next frame to match the expected next times.
        last = ds.time[-1]
        xnext = last + interval
        shift = int(np.round((next - xnext) / np.timedelta64(1, 'us')))
        # if the difference is only an interval or two, then assume the scans
        # are continguous but the PPS shifted, and set the adjustment so next
        # + adj lines up with xnext.
        logger.debug("""
 next - xnext: %s
  adjust (us): %d
   frame ends: %s
interval (us): %d
scan expected: %s
adj scan strt: %s
 shift (usec): %d""",
                     pd.to_timedelta((next - xnext).data), self.adjust_time,
                     _ft(ds.time[-1]), interval_usecs, _ft(xnext), _ft(next),
                     shift)

        # include the shift in the new adjustment, then check if the
        # adjustment has grown too large or the latest shift is too large.
        self.adjust_time -= shift
        if shift == 0:
            pass
        elif abs(shift) > 2e6 + 2*interval_usecs:
            # sometimes there are consecutive scans which get shifted even by
            # two seconds, but we can be relatively confident they are
            # contiguous if there are no dummy scans between them.
            logger.error("%d usec shift from %s to %s is too large",
                         shift, _ft(ds.time[-1]), _ft(next))
        elif abs(self.adjust_time) > 5e5:
            # half second is too far out of sync
            logger.error("%d usec shift from %s to %s: "
                         "total adjustment %d usec "
                         "is too large and will be reset ",
                         shift, _ft(ds.time[-1]), _ft(next), self.adjust_time)
        else:
            logger.info("%d usec shift, for scan starting at %s, "
                        "time adjustment is now %d us", shift,
                        _ft(scan.time[0]), self.adjust_time)
            shift = 0

        if shift == 0 and self.adjust_time:
            # since adjust_time includes the latest shift, this should bring
            # the given scan into alignment with given data frame.
            scan['time'] = scan.time + np.timedelta64(self.adjust_time, 'us')

        return shift == 0

    def get_interval(self, ds: xr.Dataset) -> np.timedelta64:
        "Return microseconds between scans, the inverse of scan rate."
        td = ds.time[-1] - ds.time[-2]
        return np.timedelta64(td.data, 'us')

    def get_period_end(self, ds: xr.Dataset) -> np.timedelta64:
        """
        Return the end of time period covered by this scan, including the
        interval after the last point.
        """
        last = ds.time[-1]
        end = last + self.get_interval(ds)
        # Using item() returns a *scalar*, which in this case would be the
        # number of nanoseconds in the timedelta64.  We want the actual
        # underlying timedelta64 type, so use .data.
        return end.data

    def fix_scan(self, scan: xr.Dataset, last_scan: xr.Dataset):
        """
        If @p scan is not 1 second after @p last_scan, but otherwise the
        housekeeping diagnostics look good, then adjust the timestamps in @p
        scan to be exactly 1 second after @p last_scan.  This handles cases
        where the hotfilm software got delayed in reading the system time to
        match up with the pps_step, and so the pps_step was added to the
        incorrect second.  If any one thing needs to be fixed--pps_step,
        pps_count, fill values, timestamps--then fix everything to be
        consistent.

        This is distinct from is_contiguous() since it only handles shifts of
        seconds and not shifts due to drift.  Also, this algorithm takes
        advantage of pps_step and pps_count.
        """
        if last_scan is None or scan is None:
            return

        time_diff = (scan.time[0] - last_scan.time[0]).data
        if time_diff < 0:
            # this likely means the last scan had the wrong time and this one
            # is catching up, which is a problem, but it's too late to do
            # anything about it.
            self.notice(scan).warning(
                "scan time %s precedes previous scan at %s" %
                (_ft(scan.time[0]), _ft(last_scan.time[0])))
            return

        step1 = last_scan['pps_step'][0].data
        step2 = scan['pps_step'][0].data
        count1 = last_scan['pps_count'][0].data
        count2 = scan['pps_count'][0].data
        # dsecond is one second plus a small delta that should account for
        # normal drift in the labjack clock relative to GPS.  if two samples
        # differ by more than this, then the successor needs to be adjusted.
        interval = self.get_interval(last_scan)
        dsecond = np.timedelta64(1000, 'ms') + interval

        # time difference within which a successive scan can be considered
        # contiguous and fixable.
        close_enough = 3*dsecond

        # from empirical observation of the raw data, it seems safe to assume
        # that if the count is -9999, but the sample time is still within a
        # few seconds of the previous, then this is still a consecutive scan.
        # Fix the count so the step and timestamps can be adjusted according
        # to the previous scan.  If somehow this is wrong, then the next scan
        # will flag a warning.
        fix_count = False
        if count2 == -9999 and time_diff <= close_enough:
            fix_count = True
            count2 = (count1 + 1) % 65536
            scan['pps_count'][0] = count2
            self.notice(scan).notice(
                "scan %s is missing pps_count, but time difference "
                "%s us is small, count set to %d" %
                (_ft(scan.time[0]), td_to_microseconds(time_diff), count2))

        # if not successive scans according to the pps variables, or the
        # sample timestamps are too far apart,then this is probably a regular
        # break in the data or a scan with bad values that will be skipped, so
        # there is nothing to be done.  time difference check guards against
        # the extremely unlikely chance that a break in the data somehow still
        # has consecutive counts.
        if (count1 + 1) % 65536 != count2 or time_diff > close_enough:
            # perhaps this is a good place to add a notice, and perhaps for
            # both last scan and this one in case they end up in different
            # files, to give an explanation on both sides of the gap...
            logger.debug("break in scans from %s (count=%d) to %s (count=%d)",
                         _ft(last_scan.time[0]), count1,
                         _ft(scan.time[0]), count2)
            # conversely, if the time difference is small but the count was
            # not consecutive, then that seems like a problem worth noting.
            if bool(time_diff <= close_enough and
                    step1 >= 0 and step2 >= 0 and
                    count1 >= 0 and count2 >= 0):
                self.notice(scan).warning(
                    "non-contiguous scan at %s with small "
                    "time difference %s us: pps count %d to %d, "
                    "pps step %d to %d" %
                    (_ft(scan.time[0]), td_to_microseconds(time_diff),
                     count1, count2, step1, step2))
            return

        # if the step changed by more than one, then likely this scan has
        # dummy values which caused the wrong pps_step to be assigned, so the
        # pps_step can be adjusted.
        fix_missing = self.skip_scan(scan)
        onesecond = np.timedelta64(1, 's')
        fix_times = time_diff < (onesecond - interval) or time_diff > dsecond

        if not fix_missing and not fix_times and not fix_count:
            # nothing found in this scan to fix...
            return

        # ok, we're fixing this scan, so fix anything that looks wrong,
        # including pps_step.
        bad_step = None
        if abs(step2 - step1) > 1:
            bad_step = int(step2)
            step2 = step1
            scan['pps_step'][0] = step2

        # it's a contiguous scan but the times are off, so correct them.
        # however, we have to be careful.  if it's only the times that are
        # wrong, meaning pps_step and pps_count look good and there are no
        # missing values, then the times were assigned relative to the wrong
        # system time and so are off by multiples of seconds.  otherwise, if
        # we don't know if pps_step was correct, then force the next scan to
        # be exactly one second after the previous.
        #
        # once a jump happens, then all the scans after it likely need to be
        # fixed also.  probably it's not useful to have a notice for every
        # fix, so they can be condensed later.
        time0 = scan.time[0]
        offset = onesecond
        jump_times = False
        if not (fix_missing or fix_count or bad_step is not None):
            # find the offset which when subtracted is close to one second
            # after the previous scan.
            offset = time_diff
            jump_times = True
            while offset > dsecond:
                offset = offset - onesecond
            if offset < (onesecond - interval):
                # something is still wrong, force to one second
                offset = onesecond
                self.notice().scantime(last_scan.time[0]+offset).warning(
                    f"scan time {time0} is {td_to_microseconds(time_diff)} us"
                    " after previous scan, cannot find integral seconds "
                    "offset for fix, so forcing to 1 second "
                    "after previous scan.")

        scan['time'] = last_scan.time + offset
        scan[self.SCAN_DIM] = last_scan[self.SCAN_DIM] + offset

        # the notice depends on whether a time jump is being fixed or a scan
        # with other wrong values
        if jump_times:
            self.time_jump_fixed(time0, scan.time[0].data)
        else:
            self.notice(scan).time_corrected_from(time0)

        # now that the scan time has been fixed, log any other notices with
        # the corrected time and fill dummy values.
        if bad_step is not None:
            self.notice(scan).notice(
                f"{_ft(scan.time[0])}: "
                f"fixed pps_step from {bad_step} to {step2}")

        if fix_missing:
            self.fill_scan(scan)

    def read_scans(self) -> Generator[xr.Dataset, None, None]:
        """
        Yield the minimum time period of scans and the following scans until
        there is a break.  A break happens for any of these reasons:
            - there are no more scans
            - keep_contiguous is false and the next scan is not contiguous
            - the sample rate changes
            - the maximum block period is reached
        The minimum block period must be reached before any scans are
        returned.  If the minimum period is not reached before a break, then
        the search starts over for the next block.
        """
        logger.debug("starting read_scans()...")
        self.adjust_time = 0
        # accumulate scans in a list until the minimum period is reached.
        minreached = False
        scan_list = []
        last_scan = None
        period_start = None
        period = np.timedelta64(0, 'us')
        # reset sample rate so it will be set by next scan.
        self.sample_rate = 0

        # keep reading until a block of scans has been yielded or else there
        # are no more scans to read.
        while True:
            # use a pending scan if set, otherwise read the next one.
            scan = self.next_scan
            if scan is None:
                scan = self.get_scan()
            # the scan will either be taken, deferred, or skipped.
            self.next_scan = None
            take_scan = None
            if scan and period_start is None:
                period_start = scan.time[0].data
            if scan:
                logger.debug("handling scan %s: %d variables, "
                             "%d samples/channel, count=%d, step=%d",
                             _ft(scan.time[0]), len(scan.data_vars),
                             len(scan.time), scan['pps_count'][0].data,
                             scan['pps_step'][0].data)
            if scan and not self.sample_rate:
                self.sample_rate = len(scan.time)
                logger.debug("set sample rate: %s", self.sample_rate)

            # correct the scan time if it looks wrong, before the other
            # checks, but only if keep_contiguous is not enabled, in which
            # case the scan times are being shifted by is_contiguous().
            if not self.keep_contiguous:
                self.fix_scan(scan, last_scan)

            # now check for a break in the scans
            if scan is None:
                logger.debug("no more scans...")
                # eof
                pass
            elif self.skip_scan(scan):
                # If there are any dummy values at all, then skip the entire
                # scan (ie, all the channels in this scan).  If the labjack
                # could not keep up and fill the entire scan, then the pps
                # count also contained dummy values, in which case the
                # computed timestamp is likely wrong too.
                logger.error("skipping scan with dummy values at %s",
                             _ft(scan.time[0]))
                # if contiguous scans not required, just keep going
                if not self.keep_contiguous:
                    continue
            elif len(scan.time) != self.sample_rate:
                logger.info("ending block: sample rate changed "
                            "from %d to %d Hz", self.sample_rate,
                            len(scan.time))
                self.next_scan = scan
            elif (self.keep_contiguous and last_scan and
                  not self.is_contiguous(last_scan, scan)):
                self.next_scan = scan
            else:
                take_scan = scan

            # if the current scan passes the other checks, check the period.
            if take_scan:
                if not scan_list and not minreached:
                    logger.info("starting scan block: %s", _ft(scan.time[0]))
                period = self.get_period_end(scan) - period_start
                period = np.timedelta64(period, 's')
                if not self.maxblock or period <= self.maxblock:
                    scan_list.append(scan)
                else:
                    logger.info("maximum block period %s exceeded at %s",
                                self.maxblock, _ft(scan.time[0]))
                    take_scan = None
                    self.next_scan = scan

            last_scan = scan_list[-1] if scan_list else None

            # see if the pending scans have reached minimum period yet
            if scan_list and not minreached:
                minreached = (period >= self.minblock)
                if minreached and last_scan:
                    logger.debug("minimum block period %s reached at %s "
                                 "with scan period %s", self.minblock,
                                 _ft(last_scan.time[-1]), period)

            if scan_list and minreached:
                # flush the scan list
                for onescan in scan_list:
                    yield onescan
                scan_list.clear()

            if take_scan is None:
                # a block is ending.  if there are still scans in the list,
                # they were not enough to make a minimum block.
                if scan_list:
                    first = scan_list[0].time[0]
                    last = scan_list[-1].time[-1]
                    logger.error("block of scans is too short (%s), "
                                 "from %s to %s", period,
                                 _ft(first), _ft(last))
                    scan_list.clear()

                # reset the time adjustment for the next block
                self.adjust_time = 0

                # return if a block was yielded and but has now ended or else
                # there are no more scans left to handle
                if minreached or scan is None:
                    break

        logger.debug("read_scans() finished.")
        return None

    def get_block(self) -> Optional[xr.Dataset]:
        """
        Read a block of scans and return them as a single Dataset.
        """
        if scans := list(self.read_scans()):
            return combine_datasets(scans, ['time', self.SCAN_DIM])
        return None

    def parse_line(self, line, scan: xr.Dataset) -> Union[xr.DataArray, None]:
        """
        Parse a line of data_dump output, and either add the data to the given
        scan if it belongs to that scan, or start a new scan and return it.
        Return None if the line could not be parsed or if the sample time is
        out of range, meaning the next line should be read.
        """
        scan_in = scan
        match = _prefix_rx.match(line) if line else None
        if not match:
            # there is no reason any lines except the header should be
            # unmatched, so warn if any are found.
            if "date time" not in line:
                self.notice().warning("unmatched line: %s" % (line.strip()))
            return None

        when = _datetime_from_match(match)

        # abort as soon as we know if this sample time is out of range
        if bool(self.begin and when < self.begin or
                self.end and when > self.end):
            return None

        if bool(scan is None or
                'time' in scan.dims and when != scan.time[0] or
                self.SCAN_DIM in scan.dims and when != scan[self.SCAN_DIM][0]):
            # start a new scan
            scan = xr.Dataset()

        spsid = int(match.group('spsid'))
        if spsid == self.ADC_STATUS_ID:
            y = np.fromstring(match.group('data'), dtype=np.int32, sep=' ')
            pps_count = xr.DataArray(y[0:1], name='pps_count',
                                     coords={self.SCAN_DIM: [when]})
            pps_count.encoding['dtype'] = 'int32'
            scan['pps_count'] = pps_count
            pps_step = xr.DataArray(y[1:2], name='pps_step',
                                    coords={self.SCAN_DIM: [when]})
            pps_step.encoding['dtype'] = 'int32'
            scan['pps_step'] = pps_step
            return scan

        # otherwise this is a channel data sample
        y = np.fromstring(match.group('data'), dtype=np.float32, sep=' ')
        channel = spsid - self.CHANNEL_IDS['ch0']
        name = f"ch{channel}"
        if name not in self.channels:
            self.notice().warning("unexpected data for channel: %s" % (name))
            return None
        step = np.timedelta64(int(1e6/len(y)), 'us')
        x = [when + (i * step) for i in range(0, len(y))]
        data = xr.DataArray(y, name=name, coords={'time': x})
        data.encoding['dtype'] = 'float32'

        logger.debug("add %s to %sscan at %s", name,
                     "" if scan else "new ", _ft(when))
        scan[data.name] = data

        # note if the scan rate changed
        if scan_in and len(scan_in.time) != len(data.time):
            logger.debug("scan %s at %s: "
                         "sample rate changed from %d to %d Hz",
                         data.name, _ft(when),
                         len(scan_in.time), len(data.time))

        return scan

    def write_text(self, out):
        data = self.get_scan()
        if data is None:
            return
        out.write("time")
        for c in data.columns:
            out.write(" %s" % (c))
        out.write("\n")
        while data is not None:
            for i in range(0, len(data.time)):
                out.write("%s" % (self.format_time(data.time[i])))
                for c in data.columns:
                    out.write(" %s" % (data[c].data[i]))
                out.write("\n")
            data = self.get_scan()

    def write_text_file(self, filespec: str):
        # keep iterating over blocks of scans until an empty block is
        # returned.
        while True:
            outpath = OutputPath()
            out = None
            tfile = None
            header = None
            last = None
            # Use the same time formatter for each block, to exploit regular
            # interval to format time strings
            tformat = None
            for data in self.read_scans():
                if header is None:
                    header = data
                    when = data.time.data[0]
                    tformat = time_formatter(self.timeformat, when)
                    tfile = outpath.start(filespec, when)
                    out = open(tfile.name, "w", buffering=32*65536)
                    out.write("time")
                    for c in data.data_vars.keys():
                        out.write(" %s" % (c))
                    out.write("\n")

                # need precision-1 decimal places since precision includes the
                # integer digit of voltage.
                fmt = f" %.{self.precision-1}f"
                for i in range(0, len(data.time)):
                    out.write("%s" % (tformat(data.time.data[i])))
                    for c in data.data_vars.keys():
                        out.write(fmt % (data[c].data[i]))
                    out.write("\n")

                last = data

            if out:
                out.close()
                # insert the file length into the final filename
                period = self.get_period_end(last) - header.time[0].data
                outpath.finish(period)

            if header is None:
                break

    def _add_netcdf_attrs(self, ds: xr.Dataset) -> xr.Dataset:
        """
        Setup time coordinate and data variable attributes for netcdf output.
        """
        ds = convert_time_coordinate(ds, ds.time)
        if self.SCAN_DIM in ds.dims:
            ds = convert_time_coordinate(ds, ds[self.SCAN_DIM])
        channels = [v for v in ds.data_vars if v.startswith('ch')]
        for c in channels:
            # use conventional netcdf and ISFS attributes
            ds[c].attrs['units'] = 'V'
            ds[c].attrs['long_name'] = f'{c} bridge voltage'
            height = self.HEIGHTS[c]
            ds[c].attrs['short_name'] = f'Eb.{height}.{self.SITE}'
            ds[c].attrs['site'] = self.SITE
            ds[c].attrs['height'] = height
            ds[c].attrs['sample_rate_hz'] = np.int32(self.sample_rate)

        if 'pps_count' in ds.data_vars:
            var = ds['pps_count']
            var.attrs['long_name'] = 'PPS counter'
            var.attrs['units'] = '1'
        if 'pps_step' in ds.data_vars:
            var = ds['pps_step']
            var.attrs['units'] = '1'
            var.attrs['long_name'] = 'Index of PPS count change'

        add_history_to_dataset(ds, "dump_hotfilm", self.command_line)
        return ds

    def get_window(self, ds: xr.Dataset) -> Tuple[Optional[np.datetime64],
                                                  Optional[np.datetime64]]:
        """
        Return the file interval window containing the start of Dataset @p ds.
        """
        if ds is None or ds.time.size == 0 or not self.file_interval:
            return None, None
        begin = rdatetime(ds.time[0].data, self.file_interval)
        if begin > ds.time[0].data:
            begin -= self.file_interval
        end = begin + self.file_interval
        logger.info("file window for %s set to [%s, %s]",
                    self.file_interval, _ft(begin), _ft(end))
        return begin, end

    def _add_notices(self, ds: xr.Dataset) -> xr.Dataset:
        """
        Add current notices to this Dataset along a new time coordinate.
        Notices without times are given the time of the first sample in the
        Dataset or the preceding notice.
        """
        ctime = ds.time[0].data
        times = []
        messages = []
        notices = self.notices
        # seems more natural to have at least one notice.  otherwise, the
        # empty notices dimension defaults to an unlimited dimension of size 0
        # when written to netcdf by xarray.  and we don't want to omit the
        # notices entirely, since it's less clear whether the output file was
        # written before notices were added or just didn't have any notices.
        if not notices:
            hdn = HotfilmDataNotice()
            hdn._message = "No notices"
            notices = [hdn]
        for notice in notices:
            if notice._scantime is not None:
                ctime = notice._scantime
            times.append(ctime)
            messages.append(notice.to_string())
        ds['notices'] = xr.DataArray(messages, name='notices',
                                     coords={'time_notices': times})
        # convert the time coordinate using the same base time as the scans,
        # so the offsets in the notice times line up with the samples.
        ds = convert_time_coordinate(ds, ds.time_notices, ds.time.data[0])
        return ds

    def read_next_file_dataset(self, ds: xr.Dataset) -> xr.Dataset:
        """
        Read the scans that will fill the next file to be written, according
        to the current file interval, and return them combined into a single
        Dataset.
        """
        # the current file interval being filled
        begin, end = None, None

        # concatenate blocks in a Dataset and write to netcdf files.
        scans = [ds] if ds is not None else []
        ds = None
        for data in self.read_scans():
            scans.append(data)
            if not self.file_interval:
                continue
            if begin is None:
                begin, end = self.get_window(scans[0])
            if data.time[-1].data >= end:
                logger.debug("file window passed at %s",
                             _ft(data.time[-1].data))
                break

        # done when no data left
        if not scans or scans[0].time.size == 0:
            logger.info("finished, no more scans read.")
            return None

        logger.debug(f"combining {len(scans)} scans into dataset...")
        ds = combine_datasets(scans, ['time', self.SCAN_DIM])

        # assume less than a second of data left is leftover from a scan
        # in the previous time window and should not be written. this
        # allows processing by intervals to work in parallel without
        # overwriting a file that will be written by a different process.
        period = self.get_period_end(ds) - ds.time[0].data
        if period < np.timedelta64(1, 's'):
            logger.info("finished, less than a second of data left.")
            return None

        return ds

    def convert_to_netcdf(self, ncds: xr.Dataset) -> xr.Dataset:
        """
        Convert the given Dataset to a form suitable for writing to netcdf.
        This includes adding current notices, converting the time coordinates
        to microseconds since the first time, and adding attributes to the
        data variables.
        """
        # add notices before the time coordinates are converted
        ncds = self._add_notices(ncds)
        self.clear_notices()
        ncds = self._add_netcdf_attrs(ncds)
        return ncds

    def write_netcdf_file(self, filespec: str,
                          ds: Optional[xr.Dataset] = None) -> (
            Tuple[Optional[xr.Dataset], Optional[xr.Dataset]]):
        """
        Like write_text_file(), but write to netcdf files.  Create a time
        coordinate variable using microseconds since the first time, and
        create variables for each channel.  If filespec is None, then return
        tuple with the next Dataset to be written, already converted to netcdf
        conventions, and any Dataset left over that would not be written.
        """
        while (ds := self.read_next_file_dataset(ds)) is not None:

            period = None
            begin, end = self.get_window(ds)
            # save file start time before coordinates are converted
            starttime = ds.time[0].data

            # if file intervals not active, then write the entire dataset,
            # otherwise write the data within the current interval
            # get length in minutes before time coordinate is converted,
            if begin is None:
                period = self.get_period_end(ds) - ds.time[0].data
                ncds = ds
                ds = None
            else:
                ncds, ds = split_dataset(ds, ['time', self.SCAN_DIM], end)

            ncds = self.convert_to_netcdf(ncds)

            if filespec is None:
                return ncds, ds
            outpath = OutputPath()
            tfile = outpath.start(filespec, starttime)
            ncds.to_netcdf(tfile.name, engine='netcdf4', format='NETCDF4')
            # for file intervals, rename to the interval start
            outpath.finish(period, begin)

        return None, None
