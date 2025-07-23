
import logging
import xarray as xr
import numpy as np
from hotfilm.utils import dt_string, r_squared

from numpy.polynomial import Polynomial
import matplotlib.axes
from hotfilm.isfs_dataset import IsfsDataset


logger = logging.getLogger(__name__)


def hotfilm_voltage_to_speed(eb, a, b):
    """
    Given this relationship between hotfilm bridge voltage and wind speed:

        eb^2 = a + b * spd^0.45

    Compute the wind speed from Eb with the coefficients a and b:

        spd = ((eb^2 - a)/b)^(1/0.45)

    The coefficient names a and b match the usage in
    https://www.thermopedia.com/content/853/, they intentionally are different
    from the usual linear coefficients of ax + b, and they are determined by a
    least squares fit.
    """
    spd = ((eb**2 - a)/b)**(1/0.45)
    return spd


class HotfilmCalibration:
    """
    Create a hot film calibration and manage metadata for it.
    """
    name: str
    _num_points: int
    a: float
    b: float
    mean_interval_seconds: int
    period_seconds: int
    begin: np.datetime64
    end: np.datetime64
    eb: xr.DataArray
    spd: xr.DataArray
    u: xr.DataArray
    v: xr.DataArray
    rms: float

    def __init__(self):
        self.name = None
        self.eb = None
        self.spd_sonic = None
        self.u = None
        self.v = None
        self._num_points = None
        self.a = None
        self.b = None
        self.mean_interval_seconds = 1
        self.period_seconds = 300
        self.begin = None
        self.rms = None
        self.rsquared_linear = None
        self.rsquared_speed = None
        self.standard_error = None

    def get_name(self):
        return self.name

    def get_end_time(self, begin: np.datetime64) -> np.datetime64:
        """
        Return the end time of the calibration period, given the start time.
        """
        # use open-ended end time by subtracting 1 nanosecond
        period = np.timedelta64(self.period_seconds, 's')
        end = begin + period - np.timedelta64(1, 'ns')
        return end

    def calibrate_winds(self, sonics: IsfsDataset, eb: xr.DataArray,
                        begin: np.datetime64, period: np.timedelta64):
        """
        Using the sonic wind component variables from @p sonics and the
        hotfilm voltage variable @p eb, calibrate the voltages with the wind
        speeds.
        """
        self.period_seconds = period.astype('timedelta64[s]').astype(int)
        end = self.get_end_time(begin)
        # get the wind component variables sliced to the current time period
        u, v, w = sonics.get_wind_data(eb, list('uvw'), begin, end)
        self.u = self.resample_mean(u)
        self.v = self.resample_mean(v)
        self.w = self.resample_mean(w)
        spd = sonics.get_speed(u, w)
        return self.calibrate(spd, eb, begin, end)

    def resample_mean(self, da: xr.DataArray) -> xr.DataArray:
        """
        Resample the given DataArray to the mean over the given period,
        assuming only that time is the first dimension but not necessarily
        named 'time'.
        """
        period = f"{self.mean_interval_seconds}s"
        indexer = {da.dims[0]: period}
        means = da.resample(**indexer).mean(skipna=True, keep_attrs=True)
        # the speed variable may have a time dimension with the frequency in
        # the name, like time60, while the volts will not, so this gives both
        # of them the same time coordinate name, based only on the averaging
        # period.
        means = means.rename({f'{da.dims[0]}': f'time_mean_{period}'})
        # This is also a good time to ensure the encoding type is float32, in
        # case the source dataset has the older double type.
        means.encoding['dtype'] = 'float32'
        return means

    def calibrate(self, spd: xr.DataArray, eb: xr.DataArray,
                  begin: np.datetime64, end: np.datetime64):
        """
        Compute a calibration by fitting the voltage to the wind speed.
        """
        self.name = eb.name
        logger.debug("\neb=%s", eb)
        logger.debug("calibrating from %s to %s", begin, end)
        if len(spd) < 2:
            raise Exception(f"too few speed points: {len(spd)}")
        eb = eb.sel(**{eb.dims[0]: slice(begin, end)})
        if len(eb) < 2:
            raise Exception(f"too few voltage points: {len(eb)}")
        spd = self.resample_mean(spd)
        eb = self.resample_mean(eb)
        self.begin = begin
        return self.fit(spd, eb)

    def fit(self, spd: xr.DataArray, eb: xr.DataArray):
        """
        Given an array of hotfilm bridge voltages and a corresponding array of
        wind speeds, already resampled to a common time period, compute the
        coefficients of a least squares fit to the hotfilm_voltage_to_speed()
        function and store them in this object.  The arrays are first aligned
        on the time coordinate, then NaN values are masked out.

        The convention seems to be to find coefficients a, b by fitting
        x=spd^0.45 mapping to y=eb^2, even though what we will want to derive
        is spd as a function of eb.  The coefficients returned by
        Polynomial.fit() are in order of degree.  So as mentioned in
        hotfilm_voltage_to_speed(), a is the 0-degree coefficient and b is the
        1-degree coefficient.
        """
        logger.debug("\nspd=%s\neb=%s", spd, eb)
        # find the intersection of the time dimensions
        logger.debug("aligning spd and eb...")
        spd, eb = xr.align(spd, eb)
        # mask NaN and infinite values.  The logical and of DataArray objects
        # with different time coordinates (eg time and time60) is 2D, even
        # though the coordinates themselves are the same after the alignment
        # above.  Thus the underlying numpy boolean arrays are combined.
        mask = np.logical_and(np.isfinite(spd).data, np.isfinite(eb).data)
        logger.debug("\nmask=%s", mask)
        spd = spd[mask]
        eb = eb[mask]
        if len(spd) < 2 or len(eb) < 2 or len(spd) != len(eb):
            raise Exception(f"too few or unequal good data points: "
                            f"{len(spd)} spd, {len(eb)} eb")
        ln = f'{eb.attrs["long_name"]} ({self.mean_interval_seconds}s mean)'
        eb.attrs['long_name'] = ln
        ln = f'{spd.attrs["long_name"]} ({self.mean_interval_seconds}s mean)'
        spd.attrs['long_name'] = ln
        self.eb = eb
        self.spd_sonic = spd
        pfit = Polynomial.fit(spd**0.45, eb**2, 1)
        logger.debug("polynomial fit: %s, window=%s, domain=%s",
                     pfit, pfit.window, pfit.domain)
        pfit = pfit.convert()
        self.a, self.b = pfit.coef[0:2]
        self._num_points = len(eb)
        logger.debug("polynomial converted: a=%.2f, b=%.2f, %s, "
                     "window=%s, domain=%s",
                     self.a, self.b, pfit, pfit.window, pfit.domain)
        self.calculate_rms()
        self.calculate_rsquared()
        return self

    def calculate_rms(self) -> float:
        """
        Calculate the root mean square of the difference between the sonic
        wind speeds and the calibration curve.
        """
        if self.spd_sonic is None or self.eb is None:
            raise Exception("no data to calculate RMS")
        rms = np.sqrt(np.mean((self.spd_sonic - self.speed(self.eb))**2))
        self.rms = float(rms)
        return rms

    def calculate_rsquared(self) -> None:
        """
        Calculate diagnostics for the quality of the fit based on R-squared
        coefficient of determination and standard error of regression.  The
        linear fit variables are expoential terms of the speed and voltage
        variables, so R-squared seems more appropriate to use in the linear
        space, but this also calculates a fit in the speed vs voltage
        space.
        """
        if self.spd_sonic is None or self.eb is None:
            raise Exception("no data to calculate R^2")
        logger.debug("calculating R-squared in speed space...")
        self.rsquared_speed = r_squared(self.spd_sonic, self.speed(self.eb))
        logger.debug("calculating R-squared in linear space...")
        self.rsquared_linear = r_squared(self.spd_sonic**0.45,
                                         self.speed(self.eb)**0.45)

    def num_points(self):
        "Return the number of points used in this calibration."
        return self._num_points

    def speed(self, eb):
        """
        Given an array of hotfilm bridge voltages, compute the corresponding
        wind speeds using the stored coefficients of the least squares fit.
        """
        return hotfilm_voltage_to_speed(eb, self.a, self.b)

    def plot(self, ax: matplotlib.axes.Axes):
        """
        Plot the calibration curve on the given axes.
        """
        spd = self.spd_sonic
        eb = self.eb
        logger.debug("plotting calibration curve:\n-->eb=%s\n-->spd=%s",
                     eb, spd)
        logger.debug("a=%s, b=%s", self.a, self.b)
        ebmin = eb.min().data
        ebmax = eb.max().data
        logger.debug("min eb=%s, max eb=%s", ebmin, ebmax)
        ebline = np.linspace(ebmin, ebmax, 100)
        spdline = self.speed(ebline)
        label = f'$spd^{{0.45}} = (eb^2 - {self.a:.2f}_a)/{self.b:.2f}_b$'
        # plot the calibration curve
        ax.plot(spdline, ebline, color='red', label=label)
        # plot the data
        if self.rms is None:
            self.calculate_rms()
        label = ('mean $E_b$ vs $|(u,w)|$, RMS=%.2f m/s, $R_{{spd}}^2$=%.2f' %
                 (self.rms, self.rsquared_speed))
        ax.scatter(spd, eb, label=label)
        ax.set_xlabel(f"{spd.name} ({spd.attrs['units']})")
        ax.set_ylabel(f"{eb.name} ({eb.attrs['units']})")
        dtime = eb.coords[eb.dims[0]]
        first = dtime.data[0]
        ax.set_title(f"{dt_string(first)}")
        ax.legend()
