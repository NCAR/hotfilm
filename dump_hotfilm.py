
import sys
import subprocess as sp
import numpy as np
from pathlib import Path
import time
import logging

import pandas as pd
import re


logger = logging.getLogger(__name__)


_prefix = "2023 06 30 21:59:27.8075 200, 521    8000"


_prefix_rx = re.compile(
    r"^(?P<year>\d{4}) (?P<month>\d{2}) (?P<day>\d{2}) "
    r"(?P<hour>\d{2}):(?P<minute>\d{2}):(?P<second>\d{2}\.?\d*) *"
    r"(?P<dsmid>\d+), *(?P<spsid>\d+) *(?P<len>\d+) (?P<data>.*)$")


class ReadHotfilm:

    def __init__(self):
        self.source = "sock:192.168.1.205"
        self.cmd = None
        self.dd = None
        # default to channel 0
        self.channel = 0
        # insert a delay between samples read from a file
        self.delay = 0

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
        data = None
        while line:
            match = _prefix_rx.match(line)
            if (match and int(match.group('spsid')) == 520+self.channel):
                break
            line = self.dd.stdout.readline()
        if line:
            data = np.fromstring(match.group('data'), dtype=float, sep=' ')
            if (self.delay):
                time.sleep(self.delay)
            logger.debug(data)
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
