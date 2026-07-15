# Tests for utils module.

import datetime as dt
import numpy as np
from hotfilm import utils
from hotfilm.time_formatter import time_formatter


def test_td_to_microseconds():
    day = 24*60*60*1000000
    tests = {
        dt.timedelta(microseconds=0): 0,
        dt.timedelta(microseconds=1): 1,
        dt.timedelta(microseconds=999): 999,
        dt.timedelta(microseconds=1000): 1000,
        dt.timedelta(microseconds=1001): 1001,
        dt.timedelta(days=1): day,
        dt.timedelta(days=1, seconds=2, microseconds=1001): day + 2001001,
        dt.timedelta(microseconds=999999): 999999,
        dt.timedelta(microseconds=1000000): 1000000,
        dt.timedelta(microseconds=1000001): 1000001,
        dt.timedelta(microseconds=999999999): 999999999,
        dt.timedelta(microseconds=1000000000): 1000000000,
        dt.timedelta(microseconds=1000000001): 1000000001
    }
    for td, xusec in tests.items():
        td = np.timedelta64(td, 'us')
        assert utils.td_to_microseconds(td) == xusec
        assert isinstance(utils.td_to_microseconds(td), int)


def test_to_datetime():
    "Test conversion of np.datetime64 to datetime.datetime."
    base = np.datetime64(dt.datetime(2023, 8, 8, 18, 6, 37, 0), 'ns')
    tests = [
        (base, "2023-08-08T18:06:37.000000"),
        (base + np.timedelta64(999999, 'us'), "2023-08-08T18:06:37.999999"),
        (base + np.timedelta64(123456, 'us'), "2023-08-08T18:06:37.123456"),
        (base + np.timedelta64(123456789, 'ns'),
         "2023-08-08T18:06:37.123456"),
    ]
    for dtime, dtime_str in tests:
        when = np.datetime64(dtime)
        when_dt = utils.to_datetime(when)
        assert isinstance(when_dt, dt.datetime)
        # assert when_dt.tzinfo == dt.timezone.utc
        assert when_dt.tzinfo is None
        assert when_dt.strftime("%Y-%m-%dT%H:%M:%S.%f") == dtime_str


def test_get_minutes():
    "Test understanding of time math."
    # 20230801_164743
    begin = dt.datetime(2023, 8, 1, 16, 47, 43, microsecond=900000)
    next = dt.datetime(2023, 8, 1, 16, 47, 43, microsecond=900500)
    end = dt.datetime(2023, 8, 1, 16, 48, 43, microsecond=899500)
    interval = (next - begin) / dt.timedelta(microseconds=1)
    assert interval == 500
    period = end - begin + dt.timedelta(microseconds=500)
    seconds = period.total_seconds()
    assert seconds == 60.0
    assert seconds // 60 == 1


def test_time_format():
    tf = time_formatter(time_formatter.ISO)
    when = np.datetime64(dt.datetime(2023, 7, 23, 2, 3, 4, 765430))
    assert tf(when) == "2023-07-23T02:03:04.765430"
    tf = time_formatter("%H:%M:%S.%f")
    assert tf(when) == "02:03:04.765430"


def test_s_format():
    tf = time_formatter("%s.%f")
    when = np.datetime64(dt.datetime(2023, 8, 8, 18, 6, 37, 0))
    epoch = np.datetime64(dt.datetime(1970, 1, 1))
    assert tf(when) == "1691517997.000000"
    assert utils.td_to_seconds(when - epoch) == 1691517997
    when = np.datetime64(dt.datetime(2023, 8, 8, 18, 6, 37, 999999))
    assert tf(when) == "1691517997.999999"
