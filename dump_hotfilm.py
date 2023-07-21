
import sys
import subprocess as sp
from pathlib import Path
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

    def __init__(self):
        self.source = "sock:192.168.1.205"
        self.cmd = None
        self.dd = None
        # default to all channels, otherwise a list of channel names
        self.channels = None
        # insert a delay between samples read from a file
        self.delay = 0
        # dataframe for the current scan as it accumulates channels
        self.scan = None
        # limit output to inside the begin and end times, if set
        self.begin = None
        self.end = None

    def set_source(self, source):
        logger.info("setting source: %s", source)
        self.source = source

    def _make_cmd(self):
        self.cmd = ["data_dump", "--nodeltat", "-i", "-1,520-523",
                    self.source]
        self.delay = 0
        if Path(self.source).exists():
            self.delay = 1

    def start(self):
        self._make_cmd()
        self.dd = sp.Popen(self.cmd, stdout=sp.PIPE, text=True)

    def select_channels(self, channels: list[int]):
        self.channels = [f"ch{ch}" for ch in channels] if channels else None
        logger.debug("selected channels %s", ",".join(self.channels))

    def get_data(self):
        """
        Return the next single channel of data as a DataFrame.
        """
        data = None
        while data is None:
            line = self.dd.stdout.readline()
            if not line:
                break
            match = _prefix_rx.match(line)
            data = self.match_to_data(match)
            if (data is not None and (self.channels and
                                      data.columns[0] not in self.channels)):
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

    def get_scan(self):
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
                continue
            # If there are any dummy values at all, then skip the entire
            # frame.  If the labjack could not keep up and fill the entire
            # scan, then the pps count also contained dummy values, in which
            # case the computed timestamp is likely wrong.
            if not self.time_selected(scan):
                scan = None
            elif self.skip_scan(scan):
                logger.error("skipping scan with dummy values at %s",
                             when.isoformat())
                scan = None
        return scan

    def parse_line(self, line):
        match = _prefix_rx.match(line)
        return self.match_to_data(match)

    def match_to_data(self, match):
        if not match:
            return None
        when = datetime_from_match(match)
        y = np.fromstring(match.group('data'), dtype=float, sep=' ')
        step = dt.timedelta(microseconds=1e6/len(y))
        x = [when + (i * step) for i in range(0, len(y))]
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
                out.write("%s" % (data.index[i].isoformat()))
                for c in data.columns:
                    out.write(" %s" % (data[c][i]))
                out.write("\n")
            data = self.get_scan()


def main(argv: list[str] or None):

    parser = argparse.ArgumentParser()
    parser.add_argument("source", nargs="?", default=None)
    parser.add_argument("--netcdf", help="Write data to named netcdf file")
    parser.add_argument("--text", help="Write data in text columns to file.  "
                        "Filenames can include time specifiers, "
                        "like %Y%m%d_%H%M%S.")
    parser.add_argument("--channel", action="append", dest="channels",
                        help="Channels from 0-3, or all by default")
    parser.add_argument("--begin",
                        help="Output scans after begin, in ISO UTC format.")
    parser.add_argument("--end",
                        help="Output scans up until end, in ISO UTC format.")
    parser.add_argument("--log", choices=['debug', 'info', 'error'],
                        default='info')
    args = parser.parse_args(argv)
    source = args.source
    ncpath = args.netcdf
    textpath = args.text

    logging.basicConfig(level=logging.getLevelName(args.log.upper()))
    hf = ReadHotfilm()
    hf.set_source(source)
    hf.select_channels(args.channels)
    if args.begin:
        hf.begin = dt.datetime.fromisoformat(args.begin)
    if args.end:
        hf.end = dt.datetime.fromisoformat(args.end)
    hf.start()
    hf.delay = 0
    if not ncpath:
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
