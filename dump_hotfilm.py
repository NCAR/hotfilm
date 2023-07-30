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


# this is the data_dump prefix without the delta column
_prefix = "2023 06 30 21:59:27.8075 200, 521    8000 1 2 3 4"


_prefix_rx = re.compile(
    r"^(?P<year>\d{4}) (?P<month>\d{2}) (?P<day>\d{2}) "
    r"(?P<hour>\d{2}):(?P<minute>\d{2}):(?P<second>\d{2}\.?\d*) *"
    r"(?P<dsmid>\d+), *(?P<spsid>\d+) *(?P<len>\d+) (?P<data>.*)$")


def datetime_from_match(match):
    seconds = float(match['second'])
    usecs = int((seconds - int(seconds)) * 1000000)
    seconds = int(seconds)
    when = dt.datetime(int(match['year']), int(match['month']),
                       int(match['day']),
                       int(match['hour']), int(match['minute']),
                       seconds, usecs,
                       dt.timezone.utc)
    return when


def test_datetime_from_match():
    m = _prefix_rx.match(_prefix)
    assert m
    when = datetime_from_match(m)
    assert when
    xwhen = dt.datetime(2023, 6, 30, 21, 59, 27, 807500, dt.timezone.utc)
    assert when == xwhen


class ReadHotfilm:
    """
    Read the hotfilm 1-second time series from data_dump.
    """
    ISO = "iso"

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
        # accumulate scans into contiguous data frames
        self.frame = None
        # limit output to inside the begin and end times, if set
        self.begin = None
        self.end = None
        self.timeformat = self.ISO
        # minimum number of seconds required to consider a block good
        self.minblock = 30*60
        # maximum number of seconds to include in a block
        self.maxblock = 4*60*60
        # adjustment to the times to get them line up when they get off by one
        # sample
        self.adjust_time = 0

    def set_time_format(self, fspec):
        """
        Set the time format specifier to @p fspec.  Passing None sets it to
        the default.
        """
        if not fspec:
            self.timeformat = self.ISO
        else:
            self.timeformat = fspec

    def format_time(self, when: dt.datetime):
        if self.timeformat == self.ISO:
            return when.isoformat()
        return when.strftime(self.timeformat)

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
            line = self.dd.stdout.readline()
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
        Return true if @p scan follows right at the end of @p frame.
        """
        last = frame.index[-1]
        next = scan.index[0]
        gap = (next - last) / dt.timedelta(microseconds=1)
        gap += self.adjust_time
        adjust = 0
        # if it skips a sample or two, adjust to line them back up.
        if 1400 < gap < 1600:
            adjust -= 1000
        elif 900 < gap < 1100:
            adjust -= 500
        elif gap < 250:
            adjust += 500
        if adjust:
            gap += adjust
            self.adjust_time += adjust
            logger.info("for scan starting at %s, "
                        "time adjustment is now %d us",
                        scan.index[0].isoformat(), self.adjust_time)

        # if there is still too large a gap, then do not adjust the
        # times in the next scan, and instead reset the adjustment
        cont = (gap < 550)
        if not cont:
            logger.error("gap detected: %d us from %s to %s",
                         gap, last.isoformat(), next.isoformat())
            self.adjust_time = 0
        elif self.adjust_time:
            scan.index += dt.timedelta(microseconds=self.adjust_time)

        return cont

    def get_block(self):
        """
        Accumulate scans until there is a break, then return them.
        """
        frame = None
        while True:
            scan = self.get_scan()
            if scan is None:
                frame = self.frame
                self.frame = None
            else:
                if self.frame is None:
                    logger.info("starting scan block: %s",
                                scan.index[0].isoformat())
                    self.frame = scan
                elif scan.index[0] - self.frame.index[0] > dt.timedelta(seconds=self.maxblock):
                    logger.info("max block size reached at %s", self.frame.index[-1].isoformat())
                    frame = self.frame
                    self.frame = scan
                elif self.is_contiguous(self.frame, scan):
                    self.frame = pd.concat([self.frame, scan])
                else:
                    frame = self.frame
                    self.frame = scan
            if frame is not None:
                first = frame.index[0]
                last = frame.index[-1]
                if first + dt.timedelta(seconds=self.minblock) > last:
                    logger.error("block of scans is too short, from %s to %s ",
                                 first.isoformat(), last.isoformat())
                    frame = None
            if scan is None or frame is not None:
                break
        return frame

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
        while True:
            data = self.get_block()
            if data is None:
                break
            when = data.index[0]
            path = when.strftime(filespec)
            logger.info("writing %d seconds to file %s",
                        len(data)/2000, path)
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
            out.close()


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
    parser.add_argument("--min", type=int, default=30*60,
                        help="Minimum seconds to write into a file.")
    parser.add_argument("--max", type=int, default=60*60,
                        help="Max seconds to write into a file.")
    parser.add_argument("--netcdf", help="Write data to named netcdf file")
    parser.add_argument("--text", help="Write data in text columns to file.  "
                        "Filenames can include time specifiers, "
                        "like %%Y%%m%%d_%%H%%M%%S.")
    parser.add_argument("--timeformat",
                        help="Timestamp format, iso or %% spec pattern.  "
                        "Use %s.%f to get floating point seconds since epoch.")
    parser.add_argument("--log", choices=['debug', 'info', 'error'],
                        default='info')
    args = parser.parse_args(argv)

    logging.basicConfig(level=logging.getLevelName(args.log.upper()))
    hf.set_source(args.source)
    if args.channels:
        hf.select_channels(args.channels)
    hf.minblock = args.min
    hf.maxblock = args.max
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


_scan = """
2023 07 20 00:00:00.0395 200, 521   8000     2.4023     2.4384     2.3979     2.2848     2.2601     2.3793     2.4415     2.4093
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
    assert when.isoformat() == "2023-07-20T00:00:00.039500+00:00"
    assert x[-1] == when + (7 * dt.timedelta(microseconds=125000))
    assert y[0] == 2.4023
    assert y[-1] == 2.4093


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
