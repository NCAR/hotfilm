
import logging
from pathlib import Path
import numpy as np
import xarray as xr
import pandas as pd
import pytest

from hotfilm.hotfilm_dataset import HotfilmDataset
from hotfilm.hotfilm_calibration import HotfilmCalibration
from hotfilm.hotfilm_calibration import hotfilm_voltage_to_speed
from hotfilm.utils import r_squared

logger = logging.getLogger(__name__)

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


def get_speeds(volts: xr.DataArray, a: float, b: float) -> xr.DataArray:
    spd = xr.DataArray(hotfilm_voltage_to_speed(volts, a, b), name='spd',
                       dims='time', coords={'time': volts.coords['time']},
                       attrs={'units': 'm/s', 'long_name': 'wind speed'})
    return spd


def get_hotfilm_data(nseconds: int, ntimes: int, a: float, b: float):
    """
    Create voltage and speed datasets over a nseconds time period with ntimes
    points, using the given calibration coefficients a and b.
    """
    dtime = get_times(nseconds, ntimes)
    volts = xr.DataArray(np.linspace(2.0, 3.0, ntimes), name='ch0',
                         coords={'time': dtime},
                         attrs={'units': 'V', 'long_name': 'ch0 voltage'})
    spd = get_speeds(volts, a, b)
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


def test_simple_rsquared():
    """
    Test the r_squared function with simple data.
    """
    actual = xr.DataArray([1, 2, 3, 4, 5], dims='x')
    predicted = xr.DataArray([1, 2, 3, 4, 5], dims='x')
    assert r_squared(actual, predicted) == pytest.approx(1.0)

    predicted = xr.DataArray([0, 1, 3, 5, 6], dims='x')
    # total sum of squares is 10, residual sum of squares is 4
    assert r_squared(actual, predicted) == pytest.approx(1 - (4.0 / 10.0))

    predicted = xr.DataArray([1, 2, 3, 4], dims='x')  # different size
    with pytest.raises(ValueError):
        r_squared(actual, predicted)


_plot_rsquared = False


def plot_calibration(hfc: HotfilmCalibration):
    if not _plot_rsquared:
        return
    import matplotlib.pyplot as plt
    fig = plt.figure()
    axs = fig.subplots(2, 1, squeeze=False)
    spdplot = axs[0, 0]
    fitplot = axs[1, 0]

    spdplot.plot(hfc.eb, hfc.spd_sonic, 'o', label='sonic speed')
    spdplot.plot(hfc.eb, hfc.speed(hfc.eb), 'r-', label='predicted speed')
    spdplot.plot(hfc.eb, float(hfc.spd_sonic.mean()) * np.ones_like(hfc.eb),
                 'g--', label='mean speed')
    spdplot.plot(hfc.eb, (hfc.spd_sonic - hfc.speed(hfc.eb))**2, '.',
                 label='residual speed')
    spdplot.set_xlabel('Voltage (V)')
    spdplot.set_ylabel('Speed (m/s)')
    spdplot.set_title('Hotfilm Calibration: $R_{{spd}}^{{2}}$=%.2f, $R_{{fit}}^{{2}}$=%.2f, RMS=%.2f m/s' %
                      (hfc.rsquared_speed, hfc.rsquared_linear, hfc.rms))
    spdplot.legend()

    linvolts = hfc.eb**2
    linsonic = hfc.spd_sonic**0.45
    fitplot.plot(linvolts, linsonic, 'o', label='$spd_{{sonic}}^{{0.45}}$')
    linfit = hfc.speed(hfc.eb)**0.45
    fitplot.plot(linvolts, linfit, 'r-', label='$spd_{{fit}}^{{0.45}}$')
    fitplot.plot(linvolts, float(linsonic.mean()) * np.ones_like(linvolts),
                 'g--', label='mean')
    fitplot.plot(linvolts, (linsonic - linfit)**2, '.',
                 label='residual $R_{{fit}}^{{2}}$=%.2f' % hfc.rsquared_linear)
    fitplot.plot(linvolts, (linvolts - hfc.a) / hfc.b, 'k-.',
                 label=f'$spd^{{0.45}} = eb^2 * {hfc.b:.2f}_{{b}} + {hfc.a:.2f}_{{a}}$')
    fitplot.set_xlabel('$Volts^{{2}}$')
    fitplot.set_ylabel('$Speed^{{0.45}}$')
    fitplot.legend()

    plt.tight_layout()
    plt.show()


def test_rsquared():
    ntimes = 300  # 5 minutes of 1 hz data
    a, b = 2.0, 1.5
    volts, spd = get_hotfilm_data(300, ntimes, a, b)
    dtime = volts.coords['time']
    hfc = HotfilmCalibration()

    logger.debug("test_rsquared: calibrating speed with no noise...")
    hfc.calibrate(spd, volts, dtime[0], dtime[-1])
    plot_calibration(hfc)
    # since we calculated speed directly from voltage, both rsquared
    # values should be 1.0
    assert hfc.rsquared_speed == pytest.approx(1.0)
    assert hfc.rsquared_linear == pytest.approx(1.0)

    logger.debug("test_rsquared: calibrating speed with noise...")
    # add noise to the volts so the fit should be the same and the mean
    # should be the same, but the rsquared should be lower and predictable.
    noise = 1
    noise_array = np.ones_like(spd)
    noise_array[np.arange(len(noise_array)) % 2 == 1] = -1
    noise_array = noise_array * noise
    logger.debug("spd before adding noise:\n%s", spd)
    spd_noisy = spd.copy()
    spd_noisy += noise_array

    logger.debug("testing speed with noise:\n%s", spd_noisy)
    assert noise_array.mean() == pytest.approx(0)
    assert noise_array.std() == pytest.approx(noise)

    logger.debug("computing rsquared sums manually from noise...")
    # use the sonic means to test rsquared, same as HotfilmCalibration
    spd_ss = np.var(spd_noisy) * len(spd_noisy)
    # verify that the formula used in calculate_rsquared() is in fact the
    # same as the variance times npoints.
    spd_ss_expected = np.sum((spd_noisy - spd_noisy.mean())**2)
    assert spd_ss == pytest.approx(spd_ss_expected)

    # residual speed should be just the noise
    res_ss = np.sum(noise_array**2)

    logger.debug("%s", hfc.spd_sonic)
    logger.debug("spd_mean=%f; res_ss=%s; spd_total_ss=%s",
                 float(hfc.spd_sonic.mean()), float(res_ss), float(spd_ss))
    rsquared_expected = 1 - (res_ss / spd_ss)

    # resample noise to 1s to get the xarray dimensions to match when
    # differenced with the predicted speed
    hfc.spd_sonic = hfc.resample_mean(spd_noisy)
    hfc.calculate_rsquared()
    hfc.calculate_rms()
    plot_calibration(hfc)

    assert hfc.rsquared_speed == pytest.approx(rsquared_expected)
    # rms should equal noise
    assert hfc.rms == pytest.approx(noise)

    # when calibrating with the noisy speed, the fit is different and the
    # rsquared is different, but the RMS should be within 5% of the noise.
    hfc.calibrate(spd_noisy, volts, dtime[0], dtime[-1])
    plot_calibration(hfc)
    assert hfc.rms == pytest.approx(noise, abs=noise*0.05)


if __name__ == "__main__":
    _plot_rsquared = True
    logging.basicConfig(level=logging.DEBUG)
    test_rsquared()
