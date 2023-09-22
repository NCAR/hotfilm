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


logger = logging.getLogger(__name__)


# this is the data_dump timestamp prefix that needs to be matched:
#
# 2023 06 30 21:59:27.8075 200, 521    8000 1 2 3 4

_prefix_rx = re.compile(
    r"^(?P<year>\d{4}) (?P<month>\d{2}) (?P<day>\d{2}) "
    r"(?P<hour>\d{2}):(?P<minute>\d{2}):(?P<second>\d{2}\.?\d*) *"
    r"(?P<dsmid>\d+), *(?P<spsid>\d+) *(?P<len>\d+) (?P<data>.*)$")


def datetime_from_match(match):
    # split seconds at the decimal to get microseconds
    seconds, _, usecs = match['second'].partition('.')
    seconds = int(seconds)
    usecs = int((usecs + '000000')[:6]) if usecs else 0
    when = dt.datetime(int(match['year']), int(match['month']),
                       int(match['day']),
                       int(match['hour']), int(match['minute']),
                       seconds, usecs,
                       dt.timezone.utc)
    return when


class ReadHotfilm:
    """
    Read the hotfilm 1-second time series from data_dump.
    """
    ISO = "iso"
    EPOCH = dt.datetime(1970, 1, 1, tzinfo=dt.timezone.utc)

    adjust_time: int

    def __init__(self):
        self.source = ["sock:192.168.1.220:31000"]
        self.cmd = None
        self.dd = None
        # default to all channels, otherwise a list of channel names
        self.channels = None
        # insert a delay between samples read from a file
        self.delay = 0
        # dataframe for the current scan as it accumulates channels
        self.scan = None
        # cache the start of the next block
        self.next_scan = None
        # limit output to inside the begin and end times, if set
        self.begin = None
        self.end = None
        self.timeformat = self.ISO
        # minimum number of seconds required to consider a block good
        self.minblock = 15*60
        # maximum number of seconds to include in a block
        self.maxblock = 120*60
        # adjustment to successive sample times to line them up with previous
        # samples as the labjack clock drifts relative to the system time.
        self.adjust_time = 0
        # iterator which returns the next data line
        self.line_iterator = None

    def set_time_format(self, fspec):
        """
        Set the time format specifier to @p fspec.  Passing None sets it to
        the default.
        """
        if not fspec:
            self.timeformat = self.ISO
        else:
            self.timeformat = fspec

    def set_min_max_block_minutes(self, mmin: int, mmax: int):
        self.minblock = mmin*60
        self.maxblock = mmax*60

    def format_time(self, when: dt.datetime):
        # The %s specifier to strftime does the wrong thing if TZ is not UTC.
        # Rather than modify the environment just for this, interpolate %s
        # explicitly here.
        if self.timeformat == self.ISO:
            return when.isoformat()
        mformat = self.timeformat
        if "%s" in mformat:
            seconds = int((when - self.EPOCH).total_seconds())
            mformat = self.timeformat.replace("%s", str(seconds))
        return when.strftime(mformat)

    def set_source(self, source):
        logger.info("setting sources: %s", ",".join(source))
        self.source = source

    def _make_cmd(self):
        self.cmd = ["data_dump", "--nodeltat", "-i", "-1,520-523"]
        self.cmd += self.source

    def start(self):
        self._make_cmd()
        command = " ".join(self.cmd)
        logger.info("running: %s%s", command[:60],
                    "..." if command[60:] else "")
        self.dd = sp.Popen(self.cmd, stdout=sp.PIPE, text=True)
        self.line_iterator = self.dd.stdout

    def select_channels(self, channels: list[int] or None):
        self.channels = [f"ch{ch}" for ch in channels] if channels else None
        logger.debug("selected channels: %s",
                     ",".join(self.channels) if self.channels else "all")

    def get_data(self):
        """
        Return the next selected channel as a DataFrame.
        """
        data = None
        while data is None:
            line = next(self.line_iterator, None)
            if not line:
                break
            match = _prefix_rx.match(line)
            data = self.match_to_data(match, line)
            if data is None:
                continue
            # If not yet into the selected range, skip it.
            when = data.index[0]
            if self.begin and when < self.begin:
                data = None
            elif self.end and when > self.end:
                data = None
                break
            elif (self.channels and data.columns[0] not in self.channels):
                data = None

        if data is not None and self.delay:
            time.sleep(self.delay)
        return data

    def skip_scan(self, scan):
        """
        Return True if this scan contains dummy values and should be skipped.
        """
        return (scan == -9999.0).any().any()

    def time_selected(self, scan: pd.DataFrame):
        when = scan.index[0]
        selected = not self.begin or when >= self.begin
        selected = selected and (not self.end or when <= self.end)
        return selected

    def get_scan(self) -> pd.DataFrame:
        """
        Return a DataFrame with all the channels in a single scan.
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
            when = data.index[0]
            if self.scan is None or self.scan.index[0] != when:
                logger.debug("starting new scan at %s", when)
                # the current scan, if any, is what will be returned
                scan = self.scan
                self.scan = data
            else:
                # join this channel with existing scan
                name = data.columns[0]
                logger.debug("adding %s to current scan at %s", name, when)
                self.scan[name] = data[name]
            if scan is None:
                # no full scan to return yet
                continue
            # If there are any dummy values at all, then skip the entire
            # frame.  If the labjack could not keep up and fill the entire
            # scan, then the pps count also contained dummy values, in which
            # case the computed timestamp is likely wrong too.
            if self.skip_scan(scan):
                logger.error("skipping scan with dummy values at %s",
                             when.isoformat())
                scan = None
        return scan

    def is_contiguous(self, frame: pd.DataFrame, scan: pd.DataFrame):
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
        next = scan.index[0] + dt.timedelta(microseconds=self.adjust_time)
        interval = self.get_interval(frame)
        # the expected start of the next scan is last + interval, and the
        # shift between expected time and actual time is calculated with the
        # current time adjustment included.  the shift is how much to add to
        # the next frame to match the expected next times.
        last = frame.index[-1]
        xnext = last + dt.timedelta(microseconds=interval)
        shift = (next - xnext) / dt.timedelta(microseconds=1)
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
                     (next - xnext), self.adjust_time,
                     frame.index[-1].isoformat(), interval,
                     xnext.isoformat(),
                     next, shift)

        # include the shift in the new adjustment, then check if the
        # adjustment has grown too large or the latest shift is too large.
        self.adjust_time -= shift
        if shift == 0:
            pass
        elif abs(shift) > 1e6 + 2*interval:
            logger.error("%d usec shift from %s to %s is too large",
                         shift, frame.index[-1].isoformat(), next.isoformat())
        elif abs(self.adjust_time) > 5e5:
            # half second is too far out of sync
            logger.error("%d usec shift from %s to %s: "
                         "total adjustment %d usec "
                         "is too large and will be reset ",
                         shift, frame.index[-1].isoformat(), next.isoformat(),
                         self.adjust_time)
        else:
            logger.info("%d usec shift, for scan starting at %s, "
                        "time adjustment is now %d us", shift,
                        scan.index[0].isoformat(), self.adjust_time)
            shift = 0

        if shift == 0 and self.adjust_time:
            # since adjust_time includes the latest shift, this should bring
            # the given scan into alignment with given data frame.
            scan.index += dt.timedelta(microseconds=self.adjust_time)

        return shift == 0

    def get_interval(self, frame) -> int:
        "Return microseconds between scans, the inverse of scan rate."
        td = frame.index[-1] - frame.index[-2]
        return td / dt.timedelta(microseconds=1)

    def get_period(self, frame: pd.DataFrame,
                   scan: pd.DataFrame = None) -> dt.timedelta:
        """
        Return the time period covered by this frame and given scan, including
        the interval after the last point.
        """
        if scan is None:
            scan = frame
        first = frame.index[0]
        last = scan.index[-1]
        interval = self.get_interval(frame)
        period = last - first + dt.timedelta(microseconds=interval)
        return period

    def read_scans(self):
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
        period = dt.timedelta(seconds=0)
        while True:
            scan = self.next_scan
            if scan is None:
                scan = self.get_scan()
            self.next_scan = None
            if scan is None:
                # eof
                pass
            elif not scan_list and not minreached:
                logger.info("starting scan block: %s",
                            scan.index[0].isoformat())
                scan_list.append(scan)
                period += self.get_period(scan)
            elif self.is_contiguous(last_scan, scan):
                period += self.get_period(scan)
                if period <= dt.timedelta(seconds=self.maxblock):
                    scan_list.append(scan)
                else:
                    self.next_scan = scan
            else:
                # break here, but save scan to start next block
                self.next_scan = scan

            last_scan = scan_list[-1] if scan_list else None

            if not minreached:
                minblock = dt.timedelta(seconds=self.minblock)
                minreached = (period >= minblock)
                if minreached:
                    logger.info("minimum block period %s reached at %s",
                                minblock, last_scan.index[-1].isoformat())

            if minreached and scan_list:
                # flush the scan list
                for onescan in scan_list:
                    yield onescan
                scan_list.clear()

            if scan is None or self.next_scan is not None:
                # a block is ending.  if there are still scans in the list,
                # they were not enough to make a minimum block.  if there is a
                # scan pending, then keep going with a new block.
                if scan_list:
                    first = scan_list[0].index[0]
                    last = scan_list[-1].index[-1]
                    logger.error("block of scans is too short (%s), "
                                 "from %s to %s",
                                 period, first.isoformat(), last.isoformat())
                    scan_list.clear()
                    if self.next_scan is not None:
                        continue
                break

        return None

    def get_block(self) -> pd.DataFrame:
        """
        Read a block of scans and return them as a single DataFrame.
        """
        scans = list(self.read_scans())
        if scans:
            return pd.concat(scans)
        return None

    def parse_line(self, line):
        match = _prefix_rx.match(line)
        return self.match_to_data(match)

    def match_to_data(self, match, line=None):
        if not match:
            return None
        when = datetime_from_match(match)
        y = np.fromstring(match.group('data'), dtype=float, sep=' ')
        step = dt.timedelta(microseconds=1e6/len(y))
        x = [when + (i * step) for i in range(0, len(y))]
        if False and line:
            logger.debug("from line: %s...; x[0]=%s, x[%d]=%s", line[:30],
                         x[0].isoformat(), len(x)-1, x[-1].isoformat())
        channel = int(match.group('spsid')) - 520
        name = f"ch{channel}"
        data = pd.DataFrame({name: y}, index=x)
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
            for i in range(0, len(data)):
                out.write("%s" % (self.format_time(data.index[i])))
                for c in data.columns:
                    out.write(" %s" % (data[c][i]))
                out.write("\n")
            data = self.get_scan()

    def write_text_file(self, filespec: str):
        # keep iterating over blocks of scans until an empty block is
        # returned.
        while True:
            out = None
            header = None
            last = None
            for data in self.read_scans():
                if header is None:
                    header = data
                    when = data.index[0]
                    path = when.strftime(filespec)
                    logger.info("writing to file: %s", path)
                    out = open(path, "w")
                    out.write("time")
                    for c in data.columns:
                        out.write(" %s" % (c))
                    out.write("\n")

                for i in range(0, len(data)):
                    out.write("%s" % (self.format_time(data.index[i])))
                    for c in data.columns:
                        out.write(" %s" % (data[c][i]))
                    out.write("\n")

                last = data

            if out:
                period = self.get_period(header, last)
                logger.info("total time in file %s: %s", path, period)
                out.close()

            if header is None:
                break


def apply_args(hf: ReadHotfilm, argv: list[str] or None):
    parser = argparse.ArgumentParser()
    parser.add_argument("source", nargs="+",
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
                        help="Minimum minutes to write into a file. (%s)" %
                        (hf.minblock//60))
    parser.add_argument("--max", type=int, default=hf.maxblock//60,
                        help="Maximum minutes to write into a file. (%d)" %
                        (hf.maxblock//60))
    parser.add_argument("--netcdf", help="Write data to named netcdf file")
    parser.add_argument("--text", help="Write data in text columns to file.  "
                        "Filenames can include time specifiers, "
                        "like %%Y%%m%%d_%%H%%M%%S.")
    parser.add_argument("--timeformat",
                        help="Timestamp format, iso or %% spec pattern.  "
                        "Use %%s.%%f for floating point seconds since epoch.")
    parser.add_argument("--log", choices=['debug', 'info', 'error'],
                        default='info')
    args = parser.parse_args(argv)

    logging.basicConfig(level=logging.getLevelName(args.log.upper()))
    hf.set_source(args.source)
    if args.channels:
        hf.select_channels(args.channels)
    hf.set_min_max_block_minutes(args.min, args.max)
    if args.begin:
        hf.begin = dt.datetime.fromisoformat(args.begin)
    if args.end:
        hf.end = dt.datetime.fromisoformat(args.end)

    hf.set_time_format(args.timeformat)
    hf.delay = args.delay
    return args


def main(argv: list[str] or None):
    hf = ReadHotfilm()
    args = apply_args(hf, argv)
    hf.start()
    if args.text:
        hf.write_text_file(args.text)
    elif args.netcdf:
        pass
    else:
        hf.write_text(sys.stdout)


if __name__ == "__main__":
    main(sys.argv[1:])


def test_datetime_from_match():
    tests = {
        "2023 06 30 21:59:27.8075 200, 521    8000 1 2 3 4":
        dt.datetime(2023, 6, 30, 21, 59, 27, 807500, dt.timezone.utc),
        "2023 06 30 21:59:27 200, 521    8000 1 2 3 4":
        dt.datetime(2023, 6, 30, 21, 59, 27, 0, dt.timezone.utc),
        "2023 06 30 21:59:27.0 200, 521    8000 1 2 3 4":
        dt.datetime(2023, 6, 30, 21, 59, 27, 0, dt.timezone.utc),
        "2023 07 20 01:02:04.3950 200, 521   8000 1 2 3 4":
        dt.datetime(2023, 7, 20, 1, 2, 4, 395000, dt.timezone.utc),
        "2023 07 20 01:02:04.000002 200, 521   8000 1 2 3 4":
        dt.datetime(2023, 7, 20, 1, 2, 4, 2, dt.timezone.utc)
    }
    for prefix, xwhen in tests.items():
        m = _prefix_rx.match(prefix)
        assert m
        when = datetime_from_match(m)
        assert when == xwhen


_scan = """
2023 07 20 01:02:03.3950 200, 521   8000     2.4023     2.4384     2.3979     2.2848     2.2601     2.3793     2.4415     2.4093
""".strip()


def test_parse_line():
    hf = ReadHotfilm()
    data = hf.parse_line(_scan)
    assert data is not None
    print(data)
    y = data['ch1']
    x = data.index
    y2 = data[data.columns[0]]
    assert (y == y2).all()
    assert len(x) == 8
    assert len(y) == 8
    when: dt.datetime
    when = x[0]
    assert when.isoformat() == "2023-07-20T01:02:03.395000+00:00"
    assert when.strftime("%Y%m%d_%H%M%S") == "20230720_010203"
    assert x[-1] == when + (7 * dt.timedelta(microseconds=125000))
    assert y[0] == 2.4023
    assert y[-1] == 2.4093


def test_get_period():
    hf = ReadHotfilm()
    data = hf.parse_line(_scan)
    period = hf.get_period(data)
    interval = hf.get_interval(data)
    assert interval == 125000
    assert period.total_seconds() == 1


_line1 = """
2023 07 20 01:02:03.0 200, 521   8000     2.4023     2.4384     2.3979     2.2848     2.2601     2.3793     2.4415     2.4093
""".strip()

_line2 = """
2023 07 20 01:02:04.0 200, 521   8000     2.4023     2.4384     2.3979     2.2848     2.2601     2.3793     2.4415     2.4093
""".strip()


def check_and_append(hf: ReadHotfilm, data: pd.DataFrame, next: pd.DataFrame,
                     xcont: bool, xadjust: int):
    """
    Test data and next for contiguousness and match result against xcont.  If
    contiguous, append next, verify interval spacing, and match next xadjust.
    """
    logger.debug("checking next scan %s contiguous: [%s, %s]",
                 "is" if xcont else "is NOT",
                 next.index[0].isoformat(),
                 next.index[-1].isoformat())
    assert hf.is_contiguous(data, next) == xcont
    if xcont:
        data = pd.concat([data, next])
        logger.debug("after appending next, data frame is: [%s, %s]",
                     data.index[0].isoformat(), data.index[-1].isoformat())
        interval = dt.timedelta(microseconds=125000)
        for i in range(1, len(data.index)):
            assert data.index[i] - data.index[i-1] == interval
        assert hf.adjust_time == xadjust
    return data


def test_is_contiguous():
    hf = ReadHotfilm()
    data = hf.parse_line(_line1)
    xfirst = dt.datetime(2023, 7, 20, 1, 2, 3, 0, dt.timezone.utc)
    assert data.index[0] == xfirst
    # after the first scan adjust should still be zero.
    assert hf.adjust_time == 0
    next = hf.parse_line(_line2)
    xfirst = dt.datetime(2023, 7, 20, 1, 2, 4, 0, dt.timezone.utc)
    assert next.index[0] == xfirst
    assert hf.adjust_time == 0
    logger.debug("test next follows at exactly the right time")
    data = check_and_append(hf, data, next, True, 0)
    logger.debug("next is shifted ahead by 2 intervals")
    interval = dt.timedelta(microseconds=125000)
    next.index += 2*interval + dt.timedelta(seconds=1)
    # still contiguous, but next is shifted back
    xadjust = -2*interval / dt.timedelta(microseconds=1)
    data = check_and_append(hf, data, next, True, xadjust)
    # if the next scan is exactly a second later, then that is like shifting 2
    # intervals relative to the previous scan, and the overall adjustment from
    # the first scan is back to 0.
    logger.debug("next follows 1 second later, shift is -250000")
    next.index += dt.timedelta(seconds=1)
    xadjust = 0
    data = check_and_append(hf, data, next, True, xadjust)
    logger.debug("next is 100 usec earlier, shift should be -100")
    next.index += dt.timedelta(microseconds=999900)
    xadjust += 100
    data = check_and_append(hf, data, next, True, xadjust)
    # finally, test that too large a shift triggers a reset
    next.index += dt.timedelta(seconds=2)
    data = check_and_append(hf, data, next, False, xadjust)


_scanfill = """
2023 07 20 00:00:00.0395 200, 521   8000     2.4023     2.4384     -9999.0     2.2848     2.2601     2.3793     2.4415     2.4093
""".strip()


def test_scan_skip():
    hf = ReadHotfilm()
    data = hf.parse_line(_scanfill)
    assert data is not None
    assert hf.skip_scan(data)


def test_time_format():
    hf = ReadHotfilm()
    when = dt.datetime(2023, 7, 23, 2, 3, 4, 765430, dt.timezone.utc)
    assert hf.format_time(when) == "2023-07-23T02:03:04.765430+00:00"
    hf.set_time_format("%H:%M:%S.%f")
    assert hf.format_time(when) == "02:03:04.765430"


def test_s_format():
    hf = ReadHotfilm()
    hf.timeformat = "%s.%f"
    when = dt.datetime(2023, 8, 8, 18, 6, 37, 0, tzinfo=dt.timezone.utc)
    epoch = dt.datetime(1970, 1, 1, tzinfo=dt.timezone.utc)
    assert hf.format_time(when) == "1691517997.000000"
    assert (when - epoch).total_seconds() == 1691517997


_block_lines = """
2023 07 20 01:02:03.0 200, 521   8000     2.4023     2.4384     2.3979     2.2848     2.2601     2.3793     2.4415     2.4093
2023 07 20 01:02:04.0 200, 521   8000     2.4023     2.4384     2.3979     2.2848     2.2601     2.3793     2.4415     2.4093
2023 07 20 01:02:05.0 200, 521   8000     2.4023     2.4384     2.3979     2.2848     2.2601     2.3793     2.4415     2.4093
2023 07 20 01:02:13.0 200, 521   8000     2.4023     2.4384     2.3979     2.2848     2.2601     2.3793     2.4415     2.4093
2023 07 20 01:02:14.0 200, 521   8000     2.4023     2.4384     2.3979     2.2848     2.2601     2.3793     2.4415     2.4093
""".strip().splitlines()


def test_get_block():
    hf = ReadHotfilm()
    hf.select_channels([1])
    hf.minblock = 0
    hf.line_iterator = iter(_block_lines)
    logger.debug("first get_block() call...")
    frame = hf.get_block()
    assert len(frame.index) == 24
    logger.debug("second get_block() call...")
    frame = hf.get_block()
    assert len(frame.index) == 16


def test_short_blocks():
    hf = ReadHotfilm()
    hf.select_channels([1])
    hf.line_iterator = iter(_block_lines)
    logger.debug("first get_block() call...")
    frame = hf.get_block()
    assert frame is None


def test_long_enough_blocks():
    hf = ReadHotfilm()
    hf.select_channels([1])
    hf.minblock = 3
    hf.line_iterator = iter(_block_lines)
    logger.debug("first get_block() call...")
    frame = hf.get_block()
    assert len(frame.index) == 24
    logger.debug("second get_block() call...")
    frame = hf.get_block()
    assert frame is None
