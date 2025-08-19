"time_formatter class for formatting time for text output."

import datetime as dt
import numpy as np
import pandas as pd

from hotfilm.utils import td_to_microseconds


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
