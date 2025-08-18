#! /bin/env python

import sys
import subprocess as sp
import argparse
import time
import logging
import re
import datetime as dt
import pandas as pd
import numpy as np
import xarray as xr
from hotfilm.outout_path import OutputPath
from hotfilm.utils import convert_time_coordinate
from hotfilm.utils import td_to_microseconds
from hotfilm.utils import add_history_to_dataset
from hotfilm.utils import rdatetime

from typing import Generator, Union
from typing import Optional, List


logger = logging.getLogger(__name__)


# this is the data_dump timestamp prefix that needs to be matched:
#
# 2023 06 30 21:59:27.8075 200, 521    8000 1 2 3 4

_prefix_rx = re.compile(
    r"^(?P<year>\d{4}) (?P<month>\d{2}) (?P<day>\d{2}) "
    r"(?P<hour>\d{2}):(?P<minute>\d{2}):(?P<second>\d{2}\.?\d*) *"
    r"(?P<dsmid>\d+), *(?P<spsid>\d+) *(?P<len>\d+) (?P<data>.*)$")


def datetime_from_match(match) -> np.datetime64:
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


def ft(dt64):
    return np.datetime_as_string(dt64, unit='us')


class time_formatter:

    ISO = "iso"
    EPOCH = np.datetime64(dt.datetime(1970, 1, 1))
    FLOAT_SECONDS = "%s.%f"

    timeformat: str
    first: np.datetime64

    def __init__(self, timeformat: str, first: np.datetime64):
        self.timeformat = timeformat
        self.first = first
        self.formatter = None
        mformat = self.timeformat
        if self.timeformat == self.ISO:
            self.formatter = self.format_iso
        elif mformat == self.FLOAT_SECONDS:
            self.formatter = self.format_sf
        elif "%s" in mformat:
            self.formatter = self.format_s
        else:
            self.formatter = self.format_strftime

    def format_strftime(self, when: np.datetime64):
        return pd.to_datetime(when).strftime(self.timeformat)

    def format_s(self, when: np.datetime64):
        "Interpolate a time format which contains %s"
        # The %s specifier to strftime does the wrong thing if TZ is not UTC.
        # Rather than modify the environment just for this, interpolate %s
        # explicitly here.
        mformat = self.timeformat
        seconds = int((when - self.EPOCH).total_seconds())
        mformat = self.timeformat.replace("%s", str(seconds))
        return when.strftime(mformat)

    def format_iso(self, when: np.datetime64):
        return pd.to_datetime(when).isoformat()

    def format_sf(self, when: np.datetime64):
        "Interpolate %s%f time format."
        usecs = td_to_microseconds(when - self.EPOCH)
        return "%d.%06d" % (usecs // 1e6, usecs % 1e6)

    def __call__(self, when):
        return self.formatter(when)


def iso_to_datetime64(iso: str) -> np.datetime64:
    """
    Convert an ISO formatted string to a datetime64.  The timezone is assumed
    to be UTC, since numpy.datetime64 does not support timezone offsets.
    """
    return np.datetime64(dt.datetime.fromisoformat(iso))


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

    # really these should come from the xml, but hardcode for now
    HEIGHTS = {
        'ch0': '0.5m',
        'ch1': '1m',
        'ch2': '2m',
        'ch3': '4m'
    }
    SITE = 't0'

    def __init__(self):
        self.source = ["sock:192.168.1.220:31000"]
        self.cmd = []
        self.dd = None
        # default to all channels, otherwise a list of channel names
        self.channels = None
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
        self.cmd = ["data_dump", "--precision", str(self.precision),
                    "--nodeltat", "-i", "-1,520-523"]
        self.cmd += self.source

    def start(self):
        self._make_cmd()
        command = " ".join(self.cmd)
        logger.info("running: %s%s", command[:60],
                    "..." if command[60:] else "")
        self.dd = sp.Popen(self.cmd, stdout=sp.PIPE, text=True)
        self.line_iterator = self.dd.stdout

    def select_channels(self, channels: Union[list[int], None]):
        self.channels = [f"ch{ch}" for ch in channels] if channels else None
        logger.debug("selected channels: %s",
                     ",".join(self.channels) if self.channels else "all")

    def get_data(self) -> Union[xr.DataArray, None]:
        """
        Return the next selected channel as a DataArray.
        """
        if not self.line_iterator:
            return None
        data = None
        while data is None:
            if not (line := next(self.line_iterator, None)):
                break
            data = self.parse_line(line)
            if data is None:
                continue
            # If not yet into the selected range, skip it.
            when = data.time[0]
            if self.begin and when < self.begin:
                data = None
            elif self.end and when > self.end:
                data = None
                break
            elif (self.channels and data.name not in self.channels):
                data = None

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
        # logger.debug("skip_scan is '%s' on data: %s", skip, scan)
        return skip

    def get_scan(self) -> Optional[xr.Dataset]:
        """
        Return a Dataset with all the channels in a single scan.  A scan is
        all channels with the same timestamp and the same sample rate.
        """
        # The full scan to be returned.
        scan = None
        while scan is None:
            data = self.get_data()
            if data is None:
                # return whatever scan might be pending
                scan = self.scan
                self.scan = None
                break
            when = data.time[0]
            if self.scan is None or self.scan.time[0] != when:
                logger.debug("scan %s at %s", data.name, ft(when))
                # the current scan, if any, is what will be returned
                scan = self.scan
                self.scan = xr.Dataset({data.name: data})
            elif len(self.scan.time) != len(data.time):
                # sample rate changed, so return the current scan
                logger.debug("scan %s at %s: "
                             "sample rate changed from %d to %d Hz",
                             data.name, ft(when),
                             len(self.scan.time), len(data.time))
                scan = self.scan
                self.scan = xr.Dataset({data.name: data})
            else:
                # join this channel with existing scan
                name = data.name
                logger.debug("add %s to scan at %s", name, ft(when))
                self.scan[name] = data
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
                     ft(ds.time[-1]), interval_usecs, ft(xnext), ft(next),
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
                         shift, ft(ds.time[-1]), ft(next))
        elif abs(self.adjust_time) > 5e5:
            # half second is too far out of sync
            logger.error("%d usec shift from %s to %s: "
                         "total adjustment %d usec "
                         "is too large and will be reset ",
                         shift, ft(ds.time[-1]), ft(next), self.adjust_time)
        else:
            logger.info("%d usec shift, for scan starting at %s, "
                        "time adjustment is now %d us", shift,
                        ft(scan.time[0]), self.adjust_time)
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
                logger.debug("handling next scan: %d channels of "
                             "%d samples at %s",
                             len(scan.data_vars), len(scan.time),
                             ft(scan.time[0]))
            if scan and not self.sample_rate:
                self.sample_rate = len(scan.time)
                logger.debug("set sample rate: %s", self.sample_rate)

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
                             ft(scan.time[0]))
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
                    logger.info("starting scan block: %s", ft(scan.time[0]))
                period = self.get_period_end(scan) - period_start
                period = np.timedelta64(period, 's')
                if not self.maxblock or period <= self.maxblock:
                    scan_list.append(scan)
                else:
                    logger.info("maximum block period %s exceeded at %s",
                                self.maxblock, ft(scan.time[0]))
                    take_scan = None
                    self.next_scan = scan

            last_scan = scan_list[-1] if scan_list else None

            # see if the pending scans have reached minimum period yet
            if scan_list and not minreached:
                minreached = (period >= self.minblock)
                if minreached and last_scan:
                    logger.info("minimum block period %s reached at %s "
                                "with scan period %s",
                                self.minblock, ft(last_scan.time[-1]), period)

            if scan_list and minreached:
                # flush the scan list
                logger.debug("yielding %d scans...", len(scan_list))
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
                                 "from %s to %s", period, ft(first), ft(last))
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
            return xr.merge(scans)
        return None

    def parse_line(self, line) -> Union[xr.DataArray, None]:
        match = _prefix_rx.match(line) if line else None
        if not match:
            return None
        when = datetime_from_match(match)
        y = np.fromstring(match.group('data'), dtype=float, sep=' ')
        step = np.timedelta64(int(1e6/len(y)), 'us')
        x = [when + (i * step) for i in range(0, len(y))]
        if False and line:
            logger.debug("from line: %s...; x[0]=%s, x[%d]=%s", line[:30],
                         x[0].isoformat(), len(x)-1, x[-1].isoformat())
        channel = int(match.group('spsid')) - 520
        name = f"ch{channel}"
        data = xr.DataArray(y, name=name, coords={'time': x})
        data.encoding['dtype'] = 'float32'
        return data

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
                    tfile = outpath.start(filespec, data)
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
        for c in ds.data_vars.keys():
            # use conventional netcdf and ISFS attributes
            ds[c].attrs['units'] = 'V'
            ds[c].attrs['long_name'] = f'{c} bridge voltage'
            height = self.HEIGHTS[c]
            ds[c].attrs['short_name'] = f'Eb.{height}.{self.SITE}'
            ds[c].attrs['site'] = self.SITE
            ds[c].attrs['height'] = height
            ds[c].attrs['sample_rate_hz'] = np.int32(self.sample_rate)

        add_history_to_dataset(ds, "dump_hotfilm", self.command_line)
        return ds

    def get_window(self, ds: xr.Dataset):
        """
        Return the file interval window containing the start of Dataset @p ds.
        """
        if ds is None or ds.time.size == 0:
            return None, None
        begin = rdatetime(ds.time[0].data, self.file_interval)
        end = begin + self.file_interval
        logger.info("file window for %s set to [%s, %s]",
                    self.file_interval, ft(begin), ft(end))
        return begin, end

    def write_netcdf_file(self, filespec: str):
        """
        Like write_text_file(), but write to a netcdf file.  Create a time
        coordinate variable using microseconds since the first time, and
        create variables for each channel.
        """
        outpath = OutputPath()
        tfile = None
        ds = None
        # the current file interval being filled
        begin: np.datetime64 = None
        end: np.datetime64 = None

        while True:
            # concatenate blocks in a Dataset and write to netcdf files.
            for data in self.read_scans():
                ds = data if ds is None else xr.concat([ds, data], dim='time')
                if not self.file_interval:
                    continue
                if begin is None:
                    begin, end = self.get_window(ds)
                if ds.time[-1].data >= end:
                    logger.debug("file window passed at %s",
                                 ft(ds.time[-1].data))
                    break

            # done when no data left
            if ds is None or ds.time.size == 0:
                break

            # if file intervals not active, then write the entire dataset,
            # otherwise write the data within the current interval
            tfile = outpath.start(filespec, ds)
            # get length in minutes before time coordinate is converted,
            # except the length is not useful on fixed file intervals.
            period = None
            if begin is None:
                period = self.get_period_end(ds) - ds.time[0].data
                ncds = ds
                ds = None
            else:
                # set window so end time is not included
                window = slice(begin, end - np.timedelta64(1, 'ns'))
                ncds = ds.sel(time=window)
                ds = ds.sel(time=slice(end, None))
            ncds = self._add_netcdf_attrs(ncds)
            # make sure data variables have type float32
            encodings = {
                var.name: {'dtype': 'float32'}
                for var in ncds.data_vars.values()
            }
            ncds.to_netcdf(tfile.name, engine='netcdf4', format='NETCDF4',
                           encoding=encodings)
            # for file intervals, rename to the interval start
            outpath.finish(period, begin)
            # advance file window or reset it according to remaining data
            begin, end = self.get_window(ds)


def apply_args(hf: ReadHotfilm, argv: Optional[List[str]]):
    parser = argparse.ArgumentParser(
        formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    parser.add_argument("input", nargs="+",
                        help="1 or more data files, or a sample "
                        "server specifier, like sock:t0t:31000.",
                        default=None)
    parser.add_argument("--channel", action="append", dest="channels",
                        default=None,
                        help="Channels from 0-3, or all by default")
    parser.add_argument("--begin",
                        help="Output scans after begin, in ISO UTC format.")
    parser.add_argument("--end",
                        help="Output scans up until end, in ISO UTC format.")
    parser.add_argument("--delay", type=float,
                        help="Wait DELAY seconds between returning scans.  "
                        "Useful for simulating real-time data when called "
                        "from the web plotting app with data files.",
                        default=0)
    parser.add_argument("--keep-contiguous", action="store_true",
                        help="Adjust sample times in contiguous blocks to "
                        "keep them exactly at the nominal sample rate, "
                        "even when the labjack clock drifts relative to GPS.")
    minminutes = np.timedelta64(hf.minblock, 'm').astype(int)
    parser.add_argument("--min", type=int, default=minminutes,
                        help="Minimum minutes to write into a file.")
    maxminutes = np.timedelta64(hf.maxblock, 'm').astype(int)
    parser.add_argument("--max", type=int, default=maxminutes,
                        help="Maximum minutes to write into a file.  "
                        "If zero, the only limit is set by --interval.")
    interval_minutes = np.timedelta64(hf.file_interval, 'm').astype(int)
    parser.add_argument("--interval", type=int, default=interval_minutes,
                        metavar="MIN",
                        help="Start netcdf files at intervals of MIN minutes.")
    parser.add_argument("--netcdf", help="Write data to named netcdf file")
    parser.add_argument("--text", help="Write data in text columns to file.  "
                        "Filenames can include time specifiers, "
                        "like %%Y%%m%%d_%%H%%M%%S.")
    parser.add_argument("--timeformat",
                        help="Timestamp format, iso or %% spec pattern.  "
                        "Use %%s.%%f for "
                        "floating point seconds since epoch.",
                        default=hf.timeformat)
    parser.add_argument("--log", choices=['debug', 'info', 'error'],
                        default='info')
    args = parser.parse_args(argv)

    if not args.text and not args.netcdf:
        parser.error("Specify output with either --text or --netcdf.")

    logging.basicConfig(level=logging.getLevelName(args.log.upper()))
    hf.set_source(args.input)
    if args.channels:
        hf.select_channels(args.channels)
    hf.set_min_max_block_minutes(args.min, args.max)
    hf.file_interval = np.timedelta64(args.interval, 'm')
    if args.begin:
        hf.begin = iso_to_datetime64(args.begin)
    if args.end:
        hf.end = iso_to_datetime64(args.end)

    hf.set_time_format(args.timeformat)
    hf.delay = args.delay
    hf.keep_contiguous = args.keep_contiguous
    return args


def main(argv: List[str]):
    hf = ReadHotfilm()
    args = apply_args(hf, argv[1:])
    # record the command line arguments for the history attribute
    hf.set_command_line(argv)
    hf.start()
    # netcdf takes precedence over text default
    if args.netcdf:
        hf.write_netcdf_file(args.netcdf)
    elif args.text:
        hf.write_text_file(args.text)
    else:
        hf.write_text(sys.stdout)


if __name__ == "__main__":
    main(sys.argv)
