"""
Tests for ReadHotfilm
"""

import subprocess as sp
import contextlib
from pathlib import Path
import logging
import datetime as dt
import pandas as pd
import numpy as np
import xarray as xr
import pytest

from dump_hotfilm import ReadHotfilm
from dump_hotfilm import time_formatter
from hotfilm.utils import td_to_microseconds
from dump_hotfilm import main

logger = logging.getLogger(__name__)


def ft(dt64):
    return np.datetime_as_string(dt64, unit='us')


def test_datetime_from_match():
    tests = {
        "2023 06 30 21:59:27.8075 200, 521    8000 1 2 3 4":
        dt.datetime(2023, 6, 30, 21, 59, 27, 807500),
        "2023 06 30 21:59:27 200, 521    8000 1 2 3 4":
        dt.datetime(2023, 6, 30, 21, 59, 27, 0),
        "2023 06 30 21:59:27.0 200, 521    8000 1 2 3 4":
        dt.datetime(2023, 6, 30, 21, 59, 27, 0),
        "2023 07 20 01:02:04.3950 200, 521   8000 1 2 3 4":
        dt.datetime(2023, 7, 20, 1, 2, 4, 395000),
        "2023 07 20 01:02:04.000002 200, 521   8000 1 2 3 4":
        dt.datetime(2023, 7, 20, 1, 2, 4, 2)
    }
    hf = ReadHotfilm()
    for line, xwhen in tests.items():
        data = hf.parse_line(line)
        assert data is not None
        assert data.time.data[0] == np.datetime64(xwhen)

# flake8: noqa: E501
_scan = """
2023 07 20 01:02:03.3950 200, 521   8000     2.4023     2.4384     2.3979     2.2848     2.2601     2.3793     2.4415     2.4093
""".strip()


def test_parse_line():
    hf = ReadHotfilm()
    ch1 = hf.parse_line(_scan)
    assert ch1 is not None
    logger.debug(ch1)
    y = ch1.data
    x = ch1.time.data
    assert len(ch1) == 8
    assert len(x) == 8
    assert len(y) == 8
    when = pd.to_datetime(x[0])
    assert when.isoformat() == "2023-07-20T01:02:03.395000"
    assert when.strftime("%Y%m%d_%H%M%S") == "20230720_010203"
    assert x[-1] == x[0] + (7 * np.timedelta64(125000, 'us'))
    assert y.data[0] == 2.4023
    assert y.data[-1] == 2.4093


def test_get_period():
    hf = ReadHotfilm()
    data = hf.parse_line(_scan)
    period = hf.get_period(data)
    interval = hf.get_interval(data)
    assert interval == 125000
    assert pd.to_timedelta(period).total_seconds() == 1


_line1 = """
2023 07 20 01:02:03.0 200, 521   8000     2.4023     2.4384     2.3979     2.2848     2.2601     2.3793     2.4415     2.4093
""".strip()

_line2 = """
2023 07 20 01:02:04.0 200, 521   8000     2.4023     2.4384     2.3979     2.2848     2.2601     2.3793     2.4415     2.4093
""".strip()


def check_and_append(hf: ReadHotfilm, data: xr.Dataset, next: xr.Dataset,
                     xcont: bool, xadjust: int):
    """
    Test data and next for contiguousness and match result against xcont.  If
    contiguous, append next, verify interval spacing, and match next xadjust.
    """
    logger.debug("checking next scan %s contiguous: [%s, %s]",
                 "is" if xcont else "is NOT",
                 ft(next.time[0]), ft(next.time[-1]))
    assert hf.is_contiguous(data, next) == xcont
    if xcont:
        data = xr.merge([data, next])
        logger.debug("after appending next, dataset is: [%s, %s]",
                     ft(data.time[0]), ft(data.time[-1]))
        interval = np.timedelta64(125000, 'us')
        for i in range(1, len(data.time)):
            assert data.time[i] - data.time[i-1] == interval
        assert hf.adjust_time == xadjust
    return data


def test_is_contiguous():
    hf = ReadHotfilm()
    data = hf.parse_line(_line1)
    xfirst = np.datetime64(dt.datetime(2023, 7, 20, 1, 2, 3, 0))
    assert data.time.data[0] == xfirst
    # after the first scan adjust should still be zero.
    assert hf.adjust_time == 0
    next = hf.parse_line(_line2)
    xfirst = np.datetime64(dt.datetime(2023, 7, 20, 1, 2, 4, 0))
    assert next.time.data[0] == xfirst
    assert hf.adjust_time == 0
    logger.debug("test next follows at exactly the right time")
    data = check_and_append(hf, data, next, True, 0)
    logger.debug("next is shifted ahead by 2 intervals")
    interval = np.timedelta64(125000, 'us')
    next['time'] = next.time + 2*interval + np.timedelta64(1, 's')
    # still contiguous, but next is shifted back
    xadjust = -2*interval / np.timedelta64(1, 'us')
    data = check_and_append(hf, data, next, True, xadjust)
    # if the next scan is exactly a second later, then that is like shifting 2
    # intervals relative to the previous scan, and the overall adjustment from
    # the first scan is back to 0.
    logger.debug("next follows 1 second later, shift is -250000")
    next['time'] = next.time + np.timedelta64(1, 's')
    xadjust = 0
    data = check_and_append(hf, data, next, True, xadjust)
    logger.debug("next is 100 usec earlier, shift should be -100")
    next['time'] = next.time + np.timedelta64(999900, 'us')
    xadjust += 100
    data = check_and_append(hf, data, next, True, xadjust)
    # finally, test that too large a shift triggers a reset
    next['time'] = next.time + np.timedelta64(2, 's')
    data = check_and_append(hf, data, next, False, xadjust)


_scanfill = """
2023 07 20 00:00:00.0395 200, 521   8000     2.4023     2.4384     -9999.0     2.2848     2.2601     2.3793     2.4415     2.4093
""".strip()


def test_scan_skip():
    hf = ReadHotfilm()
    data = hf.parse_line(_scanfill)
    assert data is not None
    ds = xr.Dataset({data.name: data})
    assert hf.skip_scan(ds)


def test_time_format():
    hf = ReadHotfilm()
    hf.timeformat = time_formatter.ISO
    when = np.datetime64(dt.datetime(2023, 7, 23, 2, 3, 4, 765430))
    assert hf.format_time(when) == "2023-07-23T02:03:04.765430"
    hf.set_time_format("%H:%M:%S.%f")
    assert hf.format_time(when) == "02:03:04.765430"


def test_s_format():
    hf = ReadHotfilm()
    hf.timeformat = "%s.%f"
    when = np.datetime64(dt.datetime(2023, 8, 8, 18, 6, 37, 0))
    epoch = np.datetime64(dt.datetime(1970, 1, 1))
    assert hf.format_time(when) == "1691517997.000000"
    assert pd.to_timedelta(when - epoch).total_seconds() == 1691517997
    when = np.datetime64(dt.datetime(2023, 8, 8, 18, 6, 37, 999999))
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
    ds = hf.get_block()
    assert len(ds.time) == 24
    logger.debug("second get_block() call...")
    ds = hf.get_block()
    assert len(ds.time) == 16


def test_short_blocks():
    hf = ReadHotfilm()
    hf.select_channels([1])
    hf.line_iterator = iter(_block_lines)
    logger.debug("first get_block() call...")
    data = hf.get_block()
    assert data is None


def test_long_enough_blocks():
    hf = ReadHotfilm()
    hf.select_channels([1])
    hf.minblock = 3
    hf.line_iterator = iter(_block_lines)
    logger.debug("first get_block() call...")
    data = hf.get_block()
    logger.debug("data returned: %s", repr(data))
    assert len(data.time) == 24
    logger.debug("second get_block() call...")
    data = hf.get_block()
    assert data is None


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
    ds = hf.get_block()
    # first block should break at the -9999
    assert ds is not None and len(ds.time) == 24
    assert ds['ch1'].data[23] == 2.4
    logger.debug("second get_block() call...")
    ds = hf.get_block()
    # should still get a block with one scan
    assert ds is not None and len(ds.time) == 8
    assert ds['ch1'].data[7] == 2.8


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


def remove_attributes(ds: xr.Dataset, attrs: list[str]) -> None:
    """
    Remove attributes from the dataset which contain any of the names in @p
    attrs.
    """
    for att in list(ds.attrs.keys()):
        if [a for a in attrs if a in att]:
            del ds.attrs[att]


_this_dir = Path(__file__).parent
_test_data_dir = Path("test_data")
_test_out_dir = Path("test_out")

def test_netcdf_output():
    datfile = _test_data_dir / "channel2_20230804_180000_05.dat"
    (_this_dir / _test_out_dir).mkdir(exist_ok=True)
    xout = _this_dir / _test_out_dir / "channel2_20230804_180000_005.nc"
    xout.unlink(missing_ok=True)
    args = ["--netcdf", _test_out_dir / "channel2_%Y%m%d_%H%M%S.nc",
            "--channel", "2", datfile]
    args = [str(arg) for arg in args]
    logger.debug("dumping: %s", " ".join(args))
    try:
        with contextlib.chdir(_this_dir):
            main(['dump_hotfilm.py'] + args)
    except FileNotFoundError as fe:
        if fe.filename == "data_dump":
            pytest.xfail("data_dump not found.")
        raise
    assert xout.exists() and xout.stat().st_size > 0
    # make sure we get permissions ugo=r also
    assert xout.stat().st_mode & 0o444 == 0o444

    xbase = _this_dir / "baseline" / "channel2_20230804_180000_005.nc"
    tds = xr.open_dataset(xout)
    xds = xr.open_dataset(xbase)
    ignores = ["history", "command_line", "version"]
    remove_attributes(tds, ignores)
    remove_attributes(xds, ignores)
    xout = xout.relative_to(Path.cwd())
    xbase = xbase.relative_to(Path.cwd())
    if tds.identical(xds):
        # record the test, knowing that it passed and no comparison needed
        assert True, f"netcdf datasets {xbase} and {xout} are identical"
        return
    # the identical() method does not show the differences, so use nc_compare
    # for that.  hardcode the path for now, but might need to be configurable.
    # at least we can know the test fails without requiring nc_compare.
    # another approach might be to implement pytest_assertrepr_compare().
    nc_compare = Path("/opt/local/bin/nc_compare")
    if not nc_compare.exists():
        pytest.fail(f"netcdf datasets {xbase} and {xout} differ, "
                    f"but {nc_compare} not found to show differences.")
    args = [nc_compare, xbase, xout,
            "--ignore", "*version",
            "--ignore", "history",
            "--ignore", "*command_line",
            "--showindex", "--showtimes", "--nans-equal", "--showequal"]
    args = [str(arg) for arg in args]
    logger.debug("comparing: %s", " ".join(args))
    assert sp.check_call(args) == 0
    pytest.fail("datasets not identical, but nc_compare returned 0!")
