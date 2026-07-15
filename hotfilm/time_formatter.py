"time_formatter class for formatting time for text output."

import datetime as dt
import numpy as np

from hotfilm.utils import td_to_microseconds, to_datetime
from hotfilm.utils import td_to_seconds


class time_formatter:

    ISO = "iso"
    EPOCH = np.datetime64(dt.datetime(1970, 1, 1), 's')
    FLOAT_SECONDS = "%s.%f"

    timeformat: str
    first: np.datetime64

    def __init__(self, timeformat: str, first: np.datetime64 = EPOCH):
        """
        Create a time_formatter with the given time format.  A %s specifier is
        replaced with the number of seconds since @p first, which defaults to
        the epoch. Call set_first() to change the first time for %s
        formatting.
        """

        self.timeformat = timeformat
        self.first = first
        self.formatter = self.format_strftime
        mformat = self.timeformat
        if self.timeformat == self.ISO:
            self.formatter = self.format_iso
        elif mformat == self.FLOAT_SECONDS:
            self.formatter = self.format_sf
        elif "%s" in mformat:
            self.formatter = self.format_s

    def set_first(self, first: np.datetime64) -> "time_formatter":
        "Set the base time for %s formatting."
        self.first = first
        return self

    def format_strftime(self, when: np.datetime64):
        return to_datetime(when).strftime(self.timeformat)

    def format_s(self, when: np.datetime64):
        "Interpolate a time format which contains %s"
        # The %s specifier to strftime does the wrong thing if TZ is not UTC.
        # Rather than modify the environment just for this, interpolate %s
        # explicitly here.
        mformat = self.timeformat
        seconds = td_to_seconds(when - self.EPOCH)
        mformat = self.timeformat.replace("%s", str(seconds))
        return to_datetime(when).strftime(mformat)

    def format_iso(self, when: np.datetime64):
        return to_datetime(when).isoformat()

    def format_sf(self, when: np.datetime64):
        "Interpolate %s%f time format."
        usecs = td_to_microseconds(when - self.EPOCH)
        return "%d.%06d" % (usecs // 1e6, usecs % 1e6)

    def __call__(self, when):
        return self.formatter(when)
