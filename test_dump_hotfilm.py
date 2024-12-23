"""
Tests for ReadHotfilm
"""

import logging
import datetime as dt
import pandas as pd

from dump_hotfilm import ReadHotfilm
from dump_hotfilm import time_formatter
from dump_hotfilm import td_to_microseconds


logger = logging.getLogger(__name__)


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
    hf = ReadHotfilm()
    for line, xwhen in tests.items():
        data = hf.parse_line(line)
        assert data is not None
        assert data.index[0] == xwhen

# flake8: noqa: E501
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
    assert y.iloc[0] == 2.4023
    assert y.iloc[-1] == 2.4093


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
    hf.timeformat = time_formatter.ISO
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
    when = dt.datetime(2023, 8, 8, 18, 6, 37, 999999, tzinfo=dt.timezone.utc)
    assert hf.format_time(when) == "1691517997.999999"


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


_skip_lines = """
2023 07 20 01:02:03.0 200, 521   8000     2.4023     2.4384     2.3979     2.2848     2.2601     2.3793     2.4415     2.4093
2023 07 20 01:02:04.0 200, 521   8000     2.4023     2.4384     2.3979     2.2848     2.2601     2.3793     2.4415     2.4093
2023 07 20 01:02:05.0 200, 521   8000     2.4023     2.4384     2.3979     2.2848     2.2601     2.3793     2.4415     2.4000
2023 07 20 01:02:06.0 200, 521   8000     2.4023     2.4384    -9999.0     2.2848     2.2601     2.3793     2.4415     2.4093
2023 07 20 01:02:07.0 200, 521   8000     2.4023     2.4384     2.3979     2.2848     2.2601     2.3793     2.4415     2.8000
""".strip().splitlines()


def test_skip_blocks():
    hf = ReadHotfilm()
    hf.select_channels([1])
    hf.minblock = 1
    hf.line_iterator = iter(_skip_lines)
    logger.debug("first get_block() call...")
    frame = hf.get_block()
    # first block should break at the -9999
    assert frame is not None and len(frame) == 24
    assert frame['ch1'].iloc[23] == 2.4
    logger.debug("second get_block() call...")
    frame = hf.get_block()
    # should still get a block with one scan
    assert frame is not None and len(frame) == 8
    assert frame['ch1'].iloc[7] == 2.8


def test_get_minutes():
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
        assert td_to_microseconds(td) == xusec
        assert isinstance(td_to_microseconds(td), int)
