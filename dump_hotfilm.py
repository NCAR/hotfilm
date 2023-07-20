
import sys
import subprocess as sp
import numpy as np
from pathlib import Path
import time
import logging
import re
import datetime as dt
import pandas as pd


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
        # default to channel 0
        self.channel = 0
        # insert a delay between samples read from a file
        self.delay = 0
        # set to true to return the spectrum instead of the time series
        self.spectrum = False
        # this will be a data frame for a single 1-second scan
        self.scan = None
        # the full frame of data to which each scan is appended
        self.frame = None

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

    def select_channel(self, ch: int):
        self.channel = ch

    def get_data(self):
        line = self.dd.stdout.readline()
        while line:
            match = _prefix_rx.match(line)
            if (match and int(match.group('spsid')) == 520+self.channel):
                break
            line = self.dd.stdout.readline()
        if line:
            return self.match_to_data(match)

    def parse_line(self, line):
        match = _prefix_rx.match(line)
        if match:
            return self.match_to_data(match)
        return None

    def match_to_data(self, match):
        when = datetime_from_match(match)
        y = np.fromstring(match.group('data'), dtype=float, sep=' ')
        step = dt.timedelta(microseconds=1e6/len(y))
        x = [when + (i * step) for i in range(0, len(y))]
        if (self.delay):
            time.sleep(self.delay)
        data = {}
        data['x'] = x
        data['y'] = y
        return data


def main(args: list[str] or None):

    source = None
    if args:
        source = args[0]

    logging.basicConfig(level=logging.DEBUG)
    hf = ReadHotfilm()
    hf.set_source(source)
    hf.select_channel(1)
    hf.start()
    data = hf.get_data()
    while True:
        if data is None:
            break
        print(data)
        data = hf.get_data()


if __name__ == "__main__":
    main(sys.argv[1:])


_scan = """
2023 07 20 00:00:00.0395 200, 521   8000     2.4023     2.4384     2.3979     2.2848     2.2601     2.3793     2.4415     2.4093
""".strip()


def test_parse_line():
    hf = ReadHotfilm()
    data = hf.parse_line(_scan)
    assert data
    y = data['y']
    x = data['x']
    assert len(x) == 8
    assert len(y) == 8
    when: dt.datetime
    when = x[0]
    assert when.isoformat() == "2023-07-20T00:00:00.039500+00:00"
    assert x[-1] == when + (7 * dt.timedelta(microseconds=125000))
    assert y[0] == 2.4023
    assert y[-1] == 2.4093
