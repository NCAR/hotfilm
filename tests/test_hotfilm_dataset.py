
from pathlib import Path
import numpy as np
import xarray as xr
import pandas as pd
import pytest

from hotfilm.hotfilm_dataset import HotfilmDataset
from hotfilm.hotfilm_calibration import HotfilmCalibration
from hotfilm.hotfilm_calibration import hotfilm_voltage_to_speed

_this_dir = Path(__file__).parent
_baseline_dir = _this_dir / "baseline"


def test_hotfilm_dataset():
    hfd = HotfilmDataset().open(_baseline_dir /
                                "channel2_20230804_180000_005.nc")
    ds = hfd.dataset
    xlen = 5*60*2000
    assert len(ds.time) == xlen
    xfirst = np.datetime64('2023-08-04T18:00:00.804500')
    assert ds.time[0].values == xfirst
    xlast = xfirst + (xlen - 1) * np.timedelta64(500, 'us')
    assert ds.time[-1].values == xlast
    hfd.dataset.close()


def get_times(nseconds: int, ntimes: int) -> pd.DatetimeIndex:
    """
    Create a time index over a nseconds time period with ntimes points.
    """
    origin = np.datetime64("2023-09-14T12:00:00", "ns")
    end = origin + np.timedelta64(nseconds, 's')
    # use ntimes+1 to get ntimes times, since the last point is omitted
    dtime = pd.date_range(origin, end, periods=ntimes+1, inclusive="left")
    return dtime


def get_hotfilm_data(nseconds: int, ntimes: int, a: float, b: float):
    """
    Create voltage and speed datasets over a nseconds time period with ntimes
    points, using the given calibration coefficients a and b.
    """
    dtime = get_times(nseconds, ntimes)
    volts = xr.DataArray(np.linspace(2.0, 3.0, ntimes), name='ch0',
                         coords={'time': dtime},
                         attrs={'units': 'V', 'long_name': 'ch0 voltage'})
    spd = xr.DataArray(hotfilm_voltage_to_speed(volts, a, b),
                       dims='time', coords={'time': dtime},
                       attrs={'units': 'm/s', 'long_name': 'wind speed'})
    return volts, spd


def test_hotfilm_calibration():
    hfc = HotfilmCalibration()
    # start with degenerate case of 1-second data for each
    ntimes = 5
    a, b = 2.0, 1.5
    volts, spd = get_hotfilm_data(ntimes, ntimes, a, b)
    dtime = volts.coords['time']

    # now if we compute the calibration, we should get the same coefficients
    hfc.calibrate(spd, volts, dtime[0], dtime[-1])
    assert hfc.num_points() == ntimes
    assert hfc.a, hfc.b == pytest.approx((a, b))


def test_misaligned_calibration():
    "Test calibration with misaligned data."
    ntimes = 5
    a, b = 2.0, 1.5
    volts, spd = get_hotfilm_data(ntimes, ntimes, a, b)
    dtime = volts.coords['time']
    hfc = HotfilmCalibration()
    hfc.calibrate(spd[:-1], volts[1:], dtime[0], dtime[-1])
    # should end up without the 2 omitted end points
    assert hfc.num_points() == ntimes - 2
    assert hfc.a, hfc.b == pytest.approx((a, b))


def test_nan_calibration():
    "Test calibration with missing data."
    ntimes = 5
    a, b = 2.0, 1.5
    volts, spd = get_hotfilm_data(ntimes, ntimes, a, b)
    dtime = volts.coords['time']
    volts[2] = np.nan
    spd[0] = np.nan
    hfc = HotfilmCalibration()
    hfc.calibrate(spd, volts, dtime[0], dtime[-1])
    # should end up with 2 fewer points
    assert hfc.num_points() == ntimes - 2
    assert hfc.a, hfc.b == pytest.approx((a, b))

    # try again with the nan colocated
    volts, spd = get_hotfilm_data(ntimes, ntimes, a, b)
    volts[2] = np.nan
    spd[2] = np.nan
    hfc = HotfilmCalibration()
    hfc.calibrate(spd, volts, dtime[0], dtime[-1])
    # should end up with only 1 fewer points
    assert hfc.num_points() == ntimes - 1
    assert hfc.a, hfc.b == pytest.approx((a, b))


def test_calibration_means():
    "Verify means are computed correctly."
    ntimes = 500*300  # 5 minutes of 500 hz data
    a, b = 2.0, 1.5
    volts, spd = get_hotfilm_data(300, ntimes, a, b)
    dtime = volts.coords['time']
    hfc = HotfilmCalibration()
    hfc.calibrate(spd, volts, dtime[0], dtime[-1])
    # should end up with 300 means
    assert hfc.num_points() == 300
    assert hfc.a, hfc.b == pytest.approx((a, b))


def test_resample():
    "Test resample to a different time period."
    ntimes = 10
    dtime = get_times(5, ntimes)
    volts = xr.DataArray(np.linspace(2.0, 3.0, ntimes, endpoint=False),
                         name='ch0', coords={'time': dtime},
                         attrs={'units': 'V', 'long_name': 'ch0 voltage'})
    assert volts.data[0] == 2.0
    assert volts.data[1] == 2.1
    assert volts.data[-1] == 2.9
    hfc = HotfilmCalibration()
    volts = hfc.resample_mean(volts)
    assert len(volts.data) == 5
    assert volts.data == pytest.approx(np.linspace(2.05, 2.85, 5))
    assert volts.dims[0] == "time_mean_1s"
