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

from hotfilm.read_hotfilm import ReadHotfilm
from hotfilm.time_formatter import time_formatter
import hotfilm.utils as utils
from dump_hotfilm import main

logger = logging.getLogger(__name__)


def ft(dt64):
    return np.datetime_as_string(dt64, unit='us')


def test_datetime_from_match():
    tests = {
        "2023-06-30T21:59:27.8075 200, 521    8000 1 2 3 4":
        dt.datetime(2023, 6, 30, 21, 59, 27, 807500),
        "2023-06-30T21:59:27 200, 521    8000 1 2 3 4":
        dt.datetime(2023, 6, 30, 21, 59, 27, 0),
        "2023-06-30T21:59:27.0 200, 521    8000 1 2 3 4":
        dt.datetime(2023, 6, 30, 21, 59, 27, 0),
        "2023-07-20T01:02:04.3950 200, 521   8000 1 2 3 4":
        dt.datetime(2023, 7, 20, 1, 2, 4, 395000),
        "2023-07-20T01:02:04.000002 200, 521   8000 1 2 3 4":
        dt.datetime(2023, 7, 20, 1, 2, 4, 2)
    }
    hf = ReadHotfilm()
    for line, xwhen in tests.items():
        data = hf.parse_line(line, None)
        assert data is not None
        assert data.time.data[0] == np.datetime64(xwhen)

# flake8: noqa: E501
_scan = """
2023-07-20T01:02:03.3950 200, 521  2.3157625  2.2800555  2.1795704  2.1745145  2.2734196  2.2863753   2.325242  2.2114854
""".strip()


def test_parse_line():
    hf = ReadHotfilm()
    scan = hf.parse_line(_scan, None)
    assert scan is not None
    ch1 = scan['ch1']
    logger.debug(ch1)
    y = ch1.data
    assert ch1.dtype == np.float32
    x = ch1.time.data
    assert len(ch1) == 8
    assert len(x) == 8
    assert len(y) == 8
    when = pd.to_datetime(x[0])
    assert when.isoformat() == "2023-07-20T01:02:03.395000"
    assert when.strftime("%Y%m%d_%H%M%S") == "20230720_010203"
    assert x[-1] == x[0] + (7 * np.timedelta64(125000, 'us'))
    assert y.data[0] == pytest.approx(2.3157625)
    assert y.data[-1] == pytest.approx(2.2114854)


_sixp = """
2023-09-20T18:15:42.843250 200, 501      24      35438        627          0         13        496     531229
""".strip()


def test_microsecond_times():
    hf = ReadHotfilm()
    scan = hf.parse_line(_sixp, None)
    assert scan is not None
    xtime = np.datetime64(dt.datetime(2023, 9, 20, 18, 15, 42, 843250))
    assert scan[hf.SCAN_DIM].data[0] == xtime


def test_get_period():
    hf = ReadHotfilm()
    data = hf.parse_line(_scan, None)
    period = (hf.get_period_end(data) - data.time[0]).data
    interval = hf.get_interval(data)
    assert interval == 125000
    assert pd.to_timedelta(period).total_seconds() == 1


_scan_half_hour = """
2023-07-20T00:32:03.3950 200, 521  2.3157625  2.2800555  2.1795704  2.1745145  2.2734196  2.2863753   2.325242  2.2114854
""".strip()


def test_get_window():
    "Make sure time window contains the start of the dataset."
    hf = ReadHotfilm()
    assert hf.file_interval == np.timedelta64(60, 'm')
    data = hf.parse_line(_scan, None)
    begin, end = hf.get_window(data)
    assert begin <= data.time[0]
    assert end > data.time[0]
    assert begin == np.datetime64(dt.datetime(2023, 7, 20, 1, 0, 0, 0))
    assert end == np.datetime64(dt.datetime(2023, 7, 20, 2, 0, 0, 0))
    # now test if the first time is closer to the end of the interval
    data = hf.parse_line(_scan_half_hour, None)
    begin, end = hf.get_window(data)
    assert begin <= data.time[0]
    assert end > data.time[0]
    assert begin == np.datetime64(dt.datetime(2023, 7, 20, 0, 0, 0, 0))
    assert end == np.datetime64(dt.datetime(2023, 7, 20, 1, 0, 0, 0))
    hf.file_interval = np.timedelta64(0, 'm')
    assert hf.get_window(data) == (None, None)


_line1 = """
2023-07-20T01:02:03.0 200, 521  2.3157625  2.2800555  2.1795704  2.1745145  2.2734196  2.2863753   2.325242  2.2114854
""".strip()

_line2 = """
2023-07-20T01:02:04.0 200, 521  2.3157625  2.2800555  2.1795704  2.1745145  2.2734196  2.2863753   2.325242  2.2114854
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
    data = hf.parse_line(_line1, None)
    xfirst = np.datetime64(dt.datetime(2023, 7, 20, 1, 2, 3, 0))
    assert data.time.data[0] == xfirst
    # after the first scan adjust should still be zero.
    assert hf.adjust_time == 0
    next = hf.parse_line(_line2, None)
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
2023-07-20T00:00:00.0395 200, 521  2.3157625  2.2800555  -9999  2.1745145  2.2734196  2.2863753   2.325242  2.2114854
""".strip()


def test_scan_skip():
    hf = ReadHotfilm()
    data = hf.parse_line(_scanfill, None)
    assert data is not None
    assert hf.skip_scan(data)


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
2023-07-20T01:02:03.0 200, 521   2.3157625  2.2800555  2.1795704  2.1745145  2.2734196  2.2863753   2.325242  2.2114854
2023-07-20T01:02:03.0 200, 501   0 0
2023-07-20T01:02:04.0 200, 521   2.3157625  2.2800555  2.1795704  2.1745145  2.2734196  2.2863753   2.325242  2.2114854
2023-07-20T01:02:04.0 200, 501   1 0
2023-07-20T01:02:05.0 200, 521   2.3157625  2.2800555  2.1795704  2.1745145  2.2734196  2.2863753   2.325242  2.2114854
2023-07-20T01:02:05.0 200, 501   2 0
2023-07-20T01:02:13.0 200, 521   2.3157625  2.2800555  2.1795704  2.1745145  2.2734196  2.2863753   2.325242  2.2114854
2023-07-20T01:02:13.0 200, 501   10 0
2023-07-20T01:02:14.0 200, 521   2.3157625  2.2800555  2.1795704  2.1745145  2.2734196  2.2863753   2.325242  2.2114854
2023-07-20T01:02:14.0 200, 501   11 0
""".strip().splitlines()


def test_get_block():
    hf = ReadHotfilm()
    hf.select_channels([1])
    hf.minblock = 0
    hf.keep_contiguous = True
    hf.line_iterator = iter(_block_lines)
    logger.debug("first get_block() call...")
    ds = hf.get_block()
    assert len(ds.time) == 24
    logger.debug("second get_block() call...")
    ds = hf.get_block()
    assert len(ds.time) == 16


def test_short_blocks():
    hf = ReadHotfilm()
    hf.set_min_max_block_minutes(1, 120)
    hf.select_channels([1])
    hf.line_iterator = iter(_block_lines)
    logger.debug("first get_block() call...")
    data = hf.get_block()
    assert data is None


def test_long_enough_blocks():
    hf = ReadHotfilm()
    hf.keep_contiguous = True
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
2023-07-20T01:02:03.0 200, 521   2.3157625  2.2800555  2.1795704  2.1745145  2.2734196  2.2863753   2.325242  2.0114854
2023-07-20T01:02:03.0 200, 501   0 0
2023-07-20T01:02:04.0 200, 521   2.3157625  2.2800555  2.1795704  2.1745145  2.2734196  2.2863753   2.325242  2.1114854
2023-07-20T01:02:04.0 200, 501   1 0
2023-07-20T01:02:05.0 200, 521   2.3157625  2.2800555  2.1795704  2.1745145  2.2734196  2.2863753   2.325242  2.2114854
2023-07-20T01:02:05.0 200, 501   2 0
2023-07-20T01:02:06.0 200, 521   2.3157625  2.2800555    -9999.0  2.1745145  2.2734196  2.2863753   2.325242  2.4114854
2023-07-20T01:02:06.0 200, 501   3 0
2023-07-20T01:02:07.0 200, 521   2.3157625  2.2800555  2.1795704  2.1745145  2.2734196  2.2863753   2.325242  2.5114854
2023-07-20T01:02:07.0 200, 501   4 0
""".strip().splitlines()


def test_skip_blocks():
    """
    This used to test that scan blocks would be broken up at the -9999, but
    now it confirms that the -9999 is filled in instead, even if the time,
    step, or count do not need to be corrected.
    """
    hf = ReadHotfilm()
    hf.select_channels([1])
    hf.minblock = 1
    hf.line_iterator = iter(_skip_lines)
    logger.debug("first get_block() call...")
    ds = hf.get_block()
    # block should not break at the -9999, but get filled in instead
    assert ds is not None and len(ds.time) == 40
    assert ds['ch1'].data[23] == pytest.approx(2.2114854)
    assert ds['ch1'].data[39] == pytest.approx(2.5114854)
    assert np.isnan(ds['ch1'].data[26])


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
        assert utils.td_to_microseconds(td) == xusec
        assert isinstance(utils.td_to_microseconds(td), int)


def remove_attributes(ds: xr.Dataset, attrs: list[str]) -> None:
    """
    Remove attributes from the dataset which contain any of the names in @p
    attrs.
    """
    for att in list(ds.attrs.keys()):
        if [a for a in attrs if a in att]:
            del ds.attrs[att]


def compare_netcdf(xout: Path, xbase: Path,
                   begin: np.datetime64 = None, end: np.datetime64 = None):
    """
    Compare two netcdf files and fail the test if and differences found.
    The expected baseline output is at @p xbase, and the test output is at
    @p xout.
    """
    assert xout.exists() and xout.stat().st_size > 0
    # make sure we get permissions ugo=r also
    assert xout.stat().st_mode & 0o444 == 0o444
    tds = xr.open_dataset(xout)
    # make sure the time dimension is increasing
    assert np.all(np.diff(tds.time.data) > np.timedelta64(0, 'us')), \
        "time dimension is not strictly increasing: " + str(xout)
    xds = xr.open_dataset(xbase)
    if begin and end:
        # can't really compare notices this way, since each file will have at
        # least one notice, so delete them first.
        del xds['notices']
        del tds['notices']
        del xds['time_notices']
        del tds['time_notices']
        window = {
            "time": slice(begin, end), "time_scan_start": slice(begin, end)
        }
        tds = tds.sel(**window)
        xds = xds.sel(**window)
    # ensure the attributes are in the output before removing them for the
    # comparision.  repo_url should be identical, but ignoring it avoids
    # changing the baseline.
    assert tds.attrs.get('history')
    assert tds.attrs.get('command_line')
    assert tds.attrs.get('repo_version')
    assert tds.attrs.get('repo_url') == utils.get_repo_url()
    ignores = ["history", "command_line", "repo_version", "repo_url"]
    # ignore the old attribute names also
    ignores += ["dump_hotfilm_version", "dump_hotfilm_command_line"]
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
            "--showindex", "--showtimes", "--nans-equal", "--showequal"]
    for att in ignores:
        args += ["--ignore", att]

    args = [str(arg) for arg in args]
    logger.debug("comparing: %s", " ".join(args))
    assert sp.check_call(args) == 0
    pytest.fail("datasets not identical, but nc_compare returned 0!")


_this_dir = Path(__file__).parent
_test_data_dir = Path("test_data")
_test_out_dir = Path("test_out")


def run_dump_hotfilm(args: list[str]) -> None:
    """
    Run dump_hotfilm.py with the given arguments from the test directory.
    """
    args = [str(arg) for arg in args]
    logger.debug("dumping: %s", " ".join(args))
    try:
        with contextlib.chdir(_this_dir):
            main(['dump_hotfilm.py'] + args)
    except FileNotFoundError as fe:
        if fe.filename == "data_dump":
            pytest.xfail("data_dump not found.")
        raise


def test_netcdf_output():
    datfile = _test_data_dir / "channel2_20230804_180000_05.dat"
    (_this_dir / _test_out_dir).mkdir(exist_ok=True)
    xout = _this_dir / _test_out_dir / "channel2_20230804_180000_005.nc"
    xout.unlink(missing_ok=True)
    args = ["--keep-contiguous", "--interval", "0",
            "--netcdf", _test_out_dir / "channel2_%Y%m%d_%H%M%S.nc",
            "--channel", "2", datfile]
    run_dump_hotfilm(args)
    xbase = _this_dir / "baseline" / "channel2_20230804_180000_005.nc"
    compare_netcdf(xout, xbase)


def test_netcdf_output_not_contiguous():
    "Just like test_netcdf_output(), but without --keep-contiguous."
    datfile = _test_data_dir / "channel2_20230804_180000_05.dat"
    (_this_dir / _test_out_dir).mkdir(exist_ok=True)
    xout = _this_dir / _test_out_dir / "channel2_skips_20230804_180000_005.nc"
    xout.unlink(missing_ok=True)
    args = ["--interval", "0", "--netcdf",
            _test_out_dir / "channel2_skips_%Y%m%d_%H%M%S.nc",
            "--channel", "2", datfile]
    run_dump_hotfilm(args)
    xbase = _this_dir / "baseline" / "channel2_skips_20230804_180000_005.nc"
    compare_netcdf(xout, xbase)


def test_netcdf_output_file_intervals():
    "Expect 5 and only 5 1-minute netcdf files."
    datfile = _test_data_dir / "channel2_20230804_180000_05.dat"
    (_this_dir / _test_out_dir).mkdir(exist_ok=True)
    xout = []
    for min in [0, 1, 2, 3, 4, 5]:
        xout.append(_this_dir / _test_out_dir /
                    f"channel2_20230804_18{min:02d}00.nc")
        xout[-1].unlink(missing_ok=True)
    args = ["--interval", "1",
            "--netcdf", _test_out_dir / "channel2_%Y%m%d_%H%M%S.nc",
            "--channel", "2", datfile]
    run_dump_hotfilm(args)
    # make sure 1805 file was not created
    assert xout[-1].exists() is False
    xbase = _this_dir / "baseline" / "channel2_skips_20230804_180000_005.nc"
    # pick one minute from the baseline and compare to the dataset in the
    # 1-minute file.  they should be the same.
    for min in [0, 1, 2, 3, 4]:
        begin = np.datetime64("2023-08-04T18:00:00") + np.timedelta64(min, 'm')
        end = begin + np.timedelta64(1, 'm') - np.timedelta64(1, 'ns')
        compare_netcdf(xout[min], xbase, begin, end)


def create_lines(when: dt.datetime, nchannels: int, nscans: int,
                 sample_rate: int) -> list[str]:
    """
    Given the number of channels in each scan, and a number of scans, and the
    given sample rate, create a list of lines with sample data.
    """
    lines = []
    fmt = "%Y-%m-%dT%H:%M:%S.000000"
    count = 0
    for iscan in range(nscans):
        for ch in range(nchannels):
            line = f"{when.strftime(fmt)} 200, {521+ch}  "
            line += " 2.4" * sample_rate
            line += "\n"
            lines.append(line)
        line = f"{when.strftime(fmt)} 200, 501   {count}  0"
        lines.append(line)
        when += dt.timedelta(seconds=1)
        count += 1
    return lines


def test_sample_rate():
    hf = ReadHotfilm()
    hf.minblock = 0
    # create test data with 2 full scans of 4 channels, first at 10 hz and
    # then at 20 hz, and make sure read_scans() breaks between them and sets
    # sample_rate correctly.
    nchannels = 4
    nscans = 2
    when = dt.datetime(2023, 7, 20, 1, 2, 3)
    datalines = create_lines(when, nchannels, nscans, 10)
    when += dt.timedelta(seconds=nscans)
    datalines.extend(create_lines(when, 4, 2, 20))
    logger.debug("test data lines:\n%s", "".join(datalines))
    hf.line_iterator = iter(datalines)
    ds = list(hf.read_scans())
    assert len(ds) == 2
    assert ds[0]
    assert ds[1]
    assert hf.sample_rate == 10
    assert len(ds[0].time) == 10
    assert len(ds[1].time) == 10

    ds = list(hf.read_scans())
    assert len(ds) == 2
    assert ds[0]
    assert ds[1]
    assert hf.sample_rate == 20
    assert len(ds[0].time) == 20
    assert len(ds[1].time) == 20

    assert not list(hf.read_scans())


def test_backwards_timestamps():
    datfile = _test_data_dir / "channel2_20230920_005950.dat"
    (_this_dir / _test_out_dir).mkdir(exist_ok=True)
    xout = _this_dir / _test_out_dir / "channel2_20230920_005950_000.nc"
    xout.unlink(missing_ok=True)
    args = ["--interval", "0", "--netcdf",
            _test_out_dir / "channel2_%Y%m%d_%H%M%S.nc",
            "--channel", "2", datfile]
    run_dump_hotfilm(args)
    xbase = _this_dir / "baseline" / "channel2_20230920_005950_000.nc"
    compare_netcdf(xout, xbase)


def test_fix_and_fill_scan():
    datfile = _test_data_dir / "hotfilm_20230920_181538.dat"
    (_this_dir / _test_out_dir).mkdir(exist_ok=True)
    xout = _this_dir / _test_out_dir / "isfs_20230920_180000.nc"
    xout.unlink(missing_ok=True)
    args = ["--netcdf", _test_out_dir / "isfs_%Y%m%d_%H%M%S.nc", datfile]
    run_dump_hotfilm(args)
    xbase = _this_dir / "baseline" / "isfs_20230920_180000.nc"
    compare_netcdf(xout, xbase)


_housekeeping_line = """
2023-09-20T00:59:55.8450 200, 501    38827        620          0         18        498     529055
""".strip()


def test_housekeeping_line():
    hf = ReadHotfilm()
    when = dt.datetime(2023, 9, 20, 0, 59, 55, 845000)
    scan = hf.parse_line(_housekeeping_line, None)
    assert scan is not None
    assert 'pps_count' in scan
    pps_count = scan['pps_count']
    assert pps_count.data[0] == 38827
    assert scan['pps_step'].data[0] == 620
    assert scan['pps_step'].dtype == np.int32
    assert scan.time_scan_start.data[0] == np.datetime64(when)


_rollover_lines = """
2023-07-20T01:02:03.0 200, 521   2.3157625  2.2800555  2.1795704  2.1745145  2.2734196  2.2863753   2.325242  2.0114854
2023-07-20T01:02:03.0 200, 501   65535 999
2023-07-20T01:02:04.0 200, 521   2.3157625  2.2800555  2.1795704  2.1745145  2.2734196  2.2863753   2.325242  2.1114854
2023-07-20T01:02:04.0 200, 501   0 999
""".strip().splitlines()

def test_rollover():
    "A rollover in pps_count is accepted as a normal consecutive scan."
    hf = ReadHotfilm()
    hf.select_channels([1])
    hf.line_iterator = iter(_rollover_lines)
    ds = hf.get_block()
    assert ds is not None and len(ds.time) == 16
    assert ds['pps_count'].data[0] == 65535
    assert ds['pps_count'].data[1] == 0
    assert ds['pps_step'].data[0] == 999
    assert ds['pps_step'].data[1] == 999
    assert hf.num_warnings() == 0
    assert hf.num_notices() == 0


_rollover_lines = """
2023-07-20T01:02:03.0 200, 521   2.3157625  2.2800555  2.1795704  2.1745145  2.2734196  2.2863753   2.325242  2.0114854
2023-07-20T01:02:03.0 200, 501   65535 999
2023-07-20T01:02:04.0 200, 521   2.3157625  2.2800555  2.1795704  2.1745145  2.2734196  2.2863753   2.325242  2.1114854
2023-07-20T01:02:04.0 200, 501   0 999
""".strip().splitlines()


# data_dump -i /,501 hotfilm_20230920_084530.dat --nodeltat --precision 8 --nolen --timeformat %Y-%m-%dT%H:%M:%S.%6f
#
# The sample times and housekeeping values are from the data_dump, the data
# values are just filled in, with dummy values inserted at the ends and
# beginning of the samples to correspond with the missing pps_count.
_missing_count_lines = """
2023-09-20T08:45:30.845500 200, 501       1226        618          1         98        514     523693
2023-09-20T08:45:30.845500 200, 520       2.3157625  2.2800555  2.1795704  2.1745145  2.2734196  2.2863753   2.325242  2.1114854
2023-09-20T08:45:31.845500 200, 501       1227        618       1586        296       1511     562063
2023-09-20T08:45:31.845500 200, 520       2.3157625  2.2800555  2.1795704  2.1745145  2.2734196  2.2863753   2.325242  2.1114854
2023-09-20T08:45:33.132500 200, 501      -9999       3470         29        985        100     990032
2023-09-20T08:45:33.132500 200, 520      2.3157625  2.2800555  2.1795704  2.1745145  2.2734196  2.2863753   -9999.0   -9999.0
2023-09-20T08:45:33.753750 200, 501       1229        985          0         57        500     619576
2023-09-20T08:45:33.753750 200, 520       -9999.0    -9999.0  2.1795704  2.1745145  2.2734196  2.2863753   2.325242  2.1114854
2023-09-20T08:45:34.845500 200, 501       1230        618          0         50        490     531054
2023-09-20T08:45:34.845500 200, 520       2.3157625  2.2800555  2.1795704  2.1745145  2.2734196  2.2863753   2.325242  2.1114854
""".strip().splitlines()

def test_missing_count():
    "A missing count value should be filled in, not cause a skip."
    hf = ReadHotfilm()
    hf.select_channels([0])
    hf.line_iterator = iter(_missing_count_lines)
    ds = hf.read_next_file_dataset(None)
    assert ds is not None and len(ds.time) == 40
    assert ds['pps_count'].data[2] == 1228
    assert ds['pps_step'].data[2] == 618
    assert ds['pps_step'].data[3] == 618
    xdata = [2.2863753, np.nan, np.nan, np.nan, np.nan, 2.1795704]
    assert ds['ch0'].data[21:27] == pytest.approx(xdata, nan_ok=True)
    when = dt.datetime(2023, 9, 20, 8, 45, 30, 845500)
    when = np.datetime64(when)
    for i in range(1, len(ds.time)):
        assert ds.time.data[i] == when + i * np.timedelta64(125, 'ms')
    for notice in hf.notices:
        logger.debug("notice: %s", notice.to_string())
    assert hf.num_warnings() == 0
    # 1 notice for the missing count, 3 each for two scans with missing data
    assert hf.num_notices() == 7
    assert hf.num_corrected() == 2


# data_dump -i /,501 -i /,521 --precision 8 --timeformat %Y-%m-%dT%H:%M:%S.%6f --nolen --nodeltat hotfilm_20230813_010000.dat | sed -E -e 's/ +/ /g' | cut  --delimiter=" " -f1-11
_time_jump = """
2023-08-13T01:01:06.696500 200, 501 7415 607 0 32 486 562987
2023-08-13T01:01:06.696500 200, 521 2.0670776 2.1700907 2.1966338 2.1710386 2.1640868 2.1267998 2.1425993 2.2165413
2023-08-13T01:01:07.696500 200, 501 7416 607 0 80 497 571568
2023-08-13T01:01:07.696500 200, 521 2.0503302 2.0203109 1.984604 1.9669085 1.9602727 1.9242498 1.91319 1.9435252
2023-08-13T01:01:08.696500 200, 501 7417 607 0 25 490 569163
2023-08-13T01:01:08.696500 200, 521 2.2108533 2.1783063 2.1950538 2.1741986 2.1331196 2.1460752 2.1144762 2.1236401
2023-08-13T01:01:10.697000 200, 501 7418 606 33 995 0 559497
2023-08-13T01:01:10.697000 200, 521 2.0942528 2.087301 2.0860372 2.0730815 2.0648656 2.0970967 2.0993087 2.0718174
2023-08-13T01:01:11.697000 200, 501 7419 606 0 19 493 -422677
2023-08-13T01:01:11.697000 200, 521 2.1410196 2.161243 2.1931579 2.1644027 2.2076936 2.2323408 2.2446644 2.2095895
2023-08-13T01:01:12.697000 200, 501 7420 606 0 67 487 -448151
2023-08-13T01:01:12.697000 200, 521 2.1413355 2.1321716 2.1508152 2.1504991 2.127116 2.0996246 2.1413355 2.1482873
""".strip().splitlines()

def test_time_jump():
    "A good sample that jumps ahead by 2 seconds needs a second subtracted."
    hf = ReadHotfilm()
    hf.select_channels([1])
    hf.line_iterator = iter(_time_jump)
    ds = hf.read_next_file_dataset(None)
    assert ds is not None and len(ds.time) == 48
    assert ds.time.data[0] == np.datetime64("2023-08-13T01:01:06.696500")
    assert ds.time.data[8] == np.datetime64("2023-08-13T01:01:07.696500")
    assert ds.time.data[16] == np.datetime64("2023-08-13T01:01:08.696500")
    assert ds.time.data[24] == np.datetime64("2023-08-13T01:01:09.697000")
    assert ds.time.data[32] == np.datetime64("2023-08-13T01:01:10.697000")
    assert ds.time.data[40] == np.datetime64("2023-08-13T01:01:11.697000")
    assert hf.num_corrected() == 3
    # but only 1 notice with 3 jumps, ending at the last scan
    assert hf.num_notices() == 1
    notice = hf.get_notices()[0]
    assert notice._scantime == np.datetime64("2023-08-13T01:01:09.697000")
    assert notice._njumps == 3
    assert notice._jump_end == np.datetime64("2023-08-13T01:01:11.697000")


_time_jump_across_hour = """
2023-08-13T01:59:56.696500 200, 501 7415 607 0 32 486 562987
2023-08-13T01:59:56.696500 200, 521 2.0670776 2.1700907 2.1966338 2.1710386 2.1640868 2.1267998 2.1425993 2.2165413
2023-08-13T01:59:57.696500 200, 501 7416 607 0 80 497 571568
2023-08-13T01:59:57.696500 200, 521 2.0503302 2.0203109 1.984604 1.9669085 1.9602727 1.9242498 1.91319 1.9435252
2023-08-13T01:59:58.696500 200, 501 7417 607 0 25 490 569163
2023-08-13T01:59:58.696500 200, 521 2.2108533 2.1783063 2.1950538 2.1741986 2.1331196 2.1460752 2.1144762 2.1236401
2023-08-13T02:00:00.697000 200, 501 7418 606 33 995 0 559497
2023-08-13T02:00:00.697000 200, 521 2.0942528 2.087301 2.0860372 2.0730815 2.0648656 2.0970967 2.0993087 2.0718174
2023-08-13T02:00:01.697000 200, 501 7419 606 0 19 493 -422677
2023-08-13T02:00:01.697000 200, 521 2.1410196 2.161243 2.1931579 2.1644027 2.2076936 2.2323408 2.2446644 2.2095895
2023-08-13T02:00:02.697000 200, 501 7420 606 0 67 487 -448151
2023-08-13T02:00:02.697000 200, 521 2.1413355 2.1321716 2.1508152 2.1504991 2.127116 2.0996246 2.1413355 2.1482873
""".strip().splitlines()


def test_time_jump_across_outputs():
    "Make sure a jump that starts in one file carries into the next."
    hf = ReadHotfilm()
    hf.select_channels([1])
    hf.line_iterator = iter(_time_jump_across_hour)
    interval = np.timedelta64(125, 'ms')
    # using the default 60m file interval, the jump should be the last scan in
    # the first file.
    ds, ds2 = hf.write_netcdf_file(None)
    # only first 3 samples of the 4th scan should be in the first file.
    assert ds is not None
    assert len(ds.time) == 27
    assert ds2 is not None
    assert len(ds2.time) == 5
    # convert the times back to datetime64
    ds = xr.decode_cf(ds)
    assert ds.time.data[0] ==  np.datetime64("2023-08-13T01:59:56.696500")
    assert ds.time.data[8] ==  np.datetime64("2023-08-13T01:59:57.696500")
    assert ds.time.data[16] == np.datetime64("2023-08-13T01:59:58.696500")
    assert ds.time.data[24] == np.datetime64("2023-08-13T01:59:59.697000")
    xlast = np.datetime64("2023-08-13T01:59:59.697000")
    xlast += 2 * interval
    assert ds.time.data[26] == xlast
    assert hf.num_corrected() == 1
    notice = hf.get_notices()[-1]
    assert notice._njumps == 1
    assert notice._jump_end == np.datetime64("2023-08-13T01:59:59.697000")

    # now get the rest
    ds, ds2 = hf.write_netcdf_file(None, ds2)
    assert ds is not None
    assert len(ds.time) == 48-27
    assert ds2 is not None
    assert len(ds2.time) == 0
    ds = xr.decode_cf(ds)
    for i in range(len(ds.time)):
        assert ds.time.data[i] == xlast + (i+1) * interval

    xnotice = "scantime=2023-08-13T02:00:00.697000; ncorrected=2; njumps=2; jump_end=2023-08-13T02:00:01.697000; message=fix scan time 2023-08-13T02:00:02.697000 to 2023-08-13T02:00:01.697000, 2 jumps since 2023-08-13T02:00:00.697000;"
    assert len(ds['notices']) == 1
    assert ds['notices'].data[0] == xnotice

    vinfo = utils.get_version_info()
    xrv = "%(version)s (%(repo_commit)s)" % (vinfo)
    assert ds.attrs['repo_version'] == xrv
    assert ds.attrs['repo_url'] == utils.get_repo_url()

    assert ds.time_scan_start.data[0] == np.datetime64("2023-08-13T02:00:00.697000")
    assert ds.time_scan_start.data[1] == np.datetime64("2023-08-13T02:00:01.697000")
    for notice in hf.get_notices():
        logger.debug("notice: %s", notice.to_string())
    # total number corrected now includes 2 more
    assert hf.num_corrected() == 3
    notice = hf.get_notices()[-1]
    assert notice._njumps == 2
    assert notice._jump_end == np.datetime64("2023-08-13T02:00:01.697000")
    assert notice.to_string() == xnotice
