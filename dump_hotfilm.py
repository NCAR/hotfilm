#! /bin/env python

import sys
import subprocess as sp
from pathlib import Path
import tempfile
import argparse
import time
import logging
import re
import datetime as dt
import pandas as pd
import numpy as np
import xarray as xr


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


_microseconds_per_seconds = 1000000
_microseconds_per_day = 24*60*60*_microseconds_per_seconds


def td_to_microseconds(td64: np.timedelta64) -> int:
    td = pd.to_timedelta(td64)
    return (td.days * _microseconds_per_day +
            td.seconds * _microseconds_per_seconds +
            td.microseconds)


class time_formatter:

    ISO = "iso"
    EPOCH = np.datetime64(dt.datetime(1970, 1, 1))
    FLOAT_SECONDS = "%s.%f"

    def __init__(self, timeformat: str, first: np.datetime64, interval: int):
        self.timeformat = timeformat
        self.first = first
        self.interval = interval
        self.formatter = None
        self.base_usecs = None
        self.i = 0
        mformat = self.timeformat
        if self.timeformat == self.ISO:
            self.formatter = self.format_iso
        elif mformat == self.FLOAT_SECONDS:
            self.base_usecs = td_to_microseconds(first - self.EPOCH)
            self.formatter = self.format_sf
            self.i = 0
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
        "Interpolate %s%f time format by exploiting regular interval."
        usecs = self.base_usecs + self.i * self.interval
        self.i += 1
        return "%d.%06d" % (usecs // 1e6, usecs % 1e6)

    def __call__(self, when):
        return self.formatter(when)


class OutputPath:

    def __init__(self):
        self.when = None
        self.path = None
        self.tfile = None

    def start(self, filespec: str, data: xr.Dataset):
        when = pd.to_datetime(data.time.data[0])
        path = Path(when.strftime(filespec))
        tfile = tempfile.NamedTemporaryFile(dir=str(path.parent),
                                            prefix=str(path)+'.',
                                            delete=False)
        logger.info("starting file: %s", tfile.name)
        self.when = when
        self.path = path
        self.tfile = tfile
        return tfile

    def finish(self, minutes: int):
        path = self.path
        fpath = path.stem + ("_%03d" % (minutes)) + path.suffix
        fpath = Path(self.tfile.name).parent / fpath
        logger.info("file finished with %d mins, renaming: %s",
                    minutes, fpath)
        fpath = Path(self.tfile.name).rename(fpath)
        # the files should not need to be writable
        fpath.chmod(0o444)
        self.tfile = None
        self.path = None
        self.when = None


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

    adjust_time: int
    scan: xr.Dataset
    next_scan: xr.Dataset

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
        self.cmd = None
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
        # minimum number of seconds required to consider a block good
        self.minblock = 1*60
        # maximum number of seconds to include in a block
        self.maxblock = 120*60
        # adjustment to successive sample times to line them up with previous
        # samples as the labjack clock drifts relative to the system time.
        self.adjust_time = 0
        # iterator which returns the next data line
        self.line_iterator = None
        self.precision = 8

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
        self.minblock = mmin*60
        self.maxblock = mmax*60

    def format_time(self, when: np.datetime64):
        "Convenient shortcut, but not optimal."
        return time_formatter(self.timeformat, when, 0)(when)

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

    def get_scan(self) -> xr.Dataset:
        """
        Return a Dataset with all the channels in a single scan.
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
                logger.debug("starting new scan at %s", ft(when))
                # the current scan, if any, is what will be returned
                scan = self.scan
                self.scan = xr.Dataset({data.name: data})
            else:
                # join this channel with existing scan
                name = data.name
                logger.debug("adding %s to current scan at %s",
                             name, ft(when))
                self.scan[name] = data
        return scan

    def is_contiguous(self, ds: xr.Dataset, scan: xr.Dataset) -> bool:
        """
        Return true if @p scan looks contiguous with @p frame, and if so,
        adjust the timestamps in @p scan accordingly.

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
        # the expected start of the next scan is last + interval, and the
        # shift between expected time and actual time is calculated with the
        # current time adjustment included.  the shift is how much to add to
        # the next frame to match the expected next times.
        last = ds.time[-1]
        xnext = last + np.timedelta64(interval, 'us')
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
                     ft(ds.time[-1]), interval, ft(xnext), ft(next), shift)

        # include the shift in the new adjustment, then check if the
        # adjustment has grown too large or the latest shift is too large.
        self.adjust_time -= shift
        if shift == 0:
            pass
        elif abs(shift) > 2e6 + 2*interval:
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

    def get_interval(self, ds: xr.Dataset) -> int:
        "Return microseconds between scans, the inverse of scan rate."
        td = ds.time[-1] - ds.time[-2]
        return int(td / np.timedelta64(1, 'us'))

    def get_period(self, ds: xr.Dataset,
                   scan: xr.Dataset = None) -> np.timedelta64:
        """
        Return the time period covered by this frame and given scan, including
        the interval after the last point.
        """
        if scan is None:
            scan = ds
        first = ds.time[0]
        last = scan.time[-1]
        interval = self.get_interval(ds)
        period = last - first + np.timedelta64(interval, 'us')
        # Using item() returns a *scalar*, which in this case would be the
        # number of nanoseconds in the timedelta64.  We want the actual
        # underlying timedelta64 type, so use .data.
        #
        # logger.debug("get_period() -> %s", period.data)
        return period.data

    def read_scans(self) -> Generator[xr.Dataset, None, None]:
        """
        Yield the minimum time period of scans and the following scans until
        there is a break.
        """
        self.adjust_time = 0
        # accumulate scans in a list until the minimum period is reached.
        minreached = False
        scan_list = []
        last_scan = None
        # period tracks the length of the current block so far
        period = np.timedelta64(0, 's')
        while True:
            scan = self.next_scan
            skipped = False
            if scan is None:
                scan = self.get_scan()
            self.next_scan = None
            if scan is None:
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
                scan = None
                skipped = True
            elif not scan_list and not minreached:
                logger.info("starting scan block: %s", ft(scan.time[0]))
                scan_list.append(scan)
                period += self.get_period(scan)
            elif self.is_contiguous(last_scan, scan):
                period += self.get_period(scan)
                if period <= np.timedelta64(self.maxblock, 's'):
                    scan_list.append(scan)
                else:
                    self.next_scan = scan
            else:
                # break here, but save scan to start next block
                self.next_scan = scan

            last_scan = scan_list[-1] if scan_list else None

            if not minreached:
                minblock = np.timedelta64(self.minblock, 's')
                minreached = (period >= minblock)
                if minreached:
                    logger.info("minimum block period %s reached at %s "
                                "with scan period %s",
                                minblock, ft(last_scan.time[-1]),
                                np.timedelta64(period, 's'))

            if minreached and scan_list:
                # flush the scan list
                for onescan in scan_list:
                    yield onescan
                scan_list.clear()

            if scan is None or self.next_scan is not None:
                # a block is ending.  if there are still scans in the list,
                # they were not enough to make a minimum block.
                if scan_list:
                    first = scan_list[0].time[0]
                    last = scan_list[-1].time[-1]
                    logger.error("block of scans is too short (%s), "
                                 "from %s to %s", period, ft(first), ft(last))
                    scan_list.clear()

                # if a block of scans has already been returned, or else there
                # is no chance of more blocks because no scan is pending and
                # this one was not skipped, then return None to signal the end
                # of this current block.
                if minreached or (self.next_scan is None and not skipped):
                    break

                # reset the time adjustment and period for the next block
                self.adjust_time = 0
                period = np.timedelta64(0, 's')

        return None

    def get_block(self) -> xr.Dataset:
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
                    interval = self.get_interval(data)
                    when = data.time.data[0]
                    tformat = time_formatter(self.timeformat, when, interval)
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
                period = pd.to_timedelta(self.get_period(header, last))
                minutes = period.total_seconds() // 60
                outpath.finish(minutes)

            if header is None:
                break

    def write_netcdf_file(self, filespec: str):
        """
        Like write_text_file(), but write to a netcdf file.  Create a time
        coordinate variable using microseconds since the first time, and
        create variables for each channel.
        """
        while True:
            outpath = OutputPath()
            tfile = None
            header = None
            last = None
            datasets = []
            when: dt.datetime = None
            base: dt.datetime = None
            # create a Dataset for each block, then concatenate them to write
            # them to a netcdf file.
            for data in self.read_scans():
                if header is None:
                    header = data
                    when = pd.to_datetime(data.time.data[0])
                    base = when.replace(microsecond=0)
                    units = ('microseconds since %s' %
                             base.strftime("%Y-%m-%d %H:%M:%S+00:00"))
                    tfile = outpath.start(filespec, data)
                ds = data
                # numerous and varied attempts failed to get xarray to encode
                # the time as microseconds since base, so do it manually.
                base = np.datetime64(base)
                td = np.array([td_to_microseconds(t)
                               for t in (data.time - base).data])
                ds = ds.assign_coords(time=("time", td))
                ds['time'].attrs['units'] = units
                ds['time'].encoding = {'dtype': 'int64'}
                for c in data.data_vars.keys():
                    # use conventional netcdf and ISFS attributes
                    ds[c].attrs['units'] = 'V'
                    ds[c].attrs['long_name'] = f'{c} bridge voltage'
                    height = self.HEIGHTS[c]
                    ds[c].attrs['short_name'] = f'Eb.{height}.{self.SITE}'
                    ds[c].attrs['site'] = self.SITE
                    ds[c].attrs['height'] = height
                logger.debug("appended dataset for netcdf output: %s", ds)
                datasets.append(ds)
                last = data

            if tfile:
                ds = xr.concat(datasets, dim='time')
                ds.to_netcdf(tfile.name)
                # insert the file length into the final filename
                minutes = self.get_period(header, last)
                minutes = pd.to_timedelta(minutes).total_seconds() // 60
                outpath.finish(minutes)

            if header is None:
                break


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
    parser.add_argument("--min", type=int, default=hf.minblock//60,
                        help="Minimum minutes to write into a file.")
    parser.add_argument("--max", type=int, default=hf.maxblock//60,
                        help="Maximum minutes to write into a file.")
    parser.add_argument("--netcdf", help="Write data to named netcdf file")
    parser.add_argument("--text", help="Write data in text columns to file.  "
                        "Filenames can include time specifiers, "
                        "like %%Y%%m%%d_%%H%%M%%S.",
                        default="hotfilm_%Y%m%d_%H%M%S.txt")
    parser.add_argument("--timeformat",
                        help="Timestamp format, iso or %% spec pattern.  "
                        "Use %%s.%%f for "
                        "floating point seconds since epoch.",
                        default=hf.timeformat)
    parser.add_argument("--log", choices=['debug', 'info', 'error'],
                        default='info')
    args = parser.parse_args(argv)

    logging.basicConfig(level=logging.getLevelName(args.log.upper()))
    hf.set_source(args.input)
    if args.channels:
        hf.select_channels(args.channels)
    hf.set_min_max_block_minutes(args.min, args.max)
    if args.begin:
        hf.begin = iso_to_datetime64(args.begin)
    if args.end:
        hf.end = iso_to_datetime64(args.end)

    hf.set_time_format(args.timeformat)
    hf.delay = args.delay
    return args


def main(argv: Optional[List[str]]):
    hf = ReadHotfilm()
    args = apply_args(hf, argv)
    hf.start()
    # netcdf takes precedence over text default
    if args.netcdf:
        hf.write_netcdf_file(args.netcdf)
    elif args.text:
        hf.write_text_file(args.text)
    else:
        hf.write_text(sys.stdout)


if __name__ == "__main__":
    main(sys.argv[1:])
