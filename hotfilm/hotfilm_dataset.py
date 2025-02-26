"""
Class and functions to read and process hotfilm netcdf data.
"""
#     Spd = c(0,2.1,5.0,5.3,9.6,10.3,14.1,18.5,20.5,25.2)
#     Eb = c(1.495, 2.03, 2.33, 2.37, 2.61, 2.64, 2.80, 2.95, 3.00, 3.13)
#     Eb = Eb*gain
#     plot(Spd, Eb, xlab="Tunnel speed (m/s)",ylab="Hot-film bridge output (V)")
#     title("Test on probe SF1 done 8 Jun 2023")
#     coef = lsfit(Spd^0.45,Eb^2)$coef
#     eb = seq(1.5,3.5,by=0.01)
#     eb = eb*gain
#     spd = ((eb^2 - coef[1])/coef[2])^(1/0.45)
#     lines(spd,eb,col=2)

import logging
import xarray as xr
import numpy as np
from numpy.polynomial import Polynomial
import matplotlib.axes
from hotfilm.isfs_dataset import IsfsDataset
from hotfilm.outout_path import OutputPath
from .utils import convert_time_coordinate

logger = logging.getLogger(__name__)


def dt_string(dt: np.datetime64) -> str:
    return np.datetime_as_string(dt, unit='s')


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


def resample_mean(da: xr.DataArray, period: str) -> xr.DataArray:
    """
    Resample the given DataArray to the mean over the given period, assuming
    only that time is the first dimension but not necessarily named 'time'.
    """
    indexer = {da.dims[0]: period}
    return da.resample(**indexer).mean(skipna=True, keep_attrs=True)


class HotfilmCalibration:
    """
    Create a hot film calibration and manage metadata for it.
    """
    CALIBRATION_TIME = 'calibration_begin_time'
    a: float
    b: float
    eb: xr.DataArray
    spd: xr.DataArray

    def __init__(self):
        self.eb = None
        self.spd = None
        self.a = None
        self.b = None
        self.mean_interval = '1s'
        self.begin = None
        self.end = None

    def as_dataset(self) -> xr.Dataset:
        """
        Return a Dataset with variables for the coefficients and a time
        coordinate.
        """
        ds = xr.Dataset()
        name = self.eb.name
        timed = xr.DataArray(name=self.CALIBRATION_TIME,
                             data=[self.begin],
                             dims=[self.CALIBRATION_TIME],
                             attrs={'long_name':
                                    'Calibration period begin time',
                                    'period': '300s',
                                    'mean_interval': self.mean_interval})

        long_name = "first-degree coefficient b: eb^2 = a + b * spd^0.45"
        units = "V**2"
        a = xr.DataArray(name=f'a_{name}', data=[self.a],
                         dims=[self.CALIBRATION_TIME],
                         coords={timed.name: timed},
                         attrs={'long_name': long_name, 'units': units})
        long_name = "zero-degree coefficient a: eb^2 = a + b * spd^0.45"
        units = "V**2 * (m/s)**-0.45"
        b = xr.DataArray(name=f'b_{name}', data=[self.b],
                         coords={timed.name: timed},
                         dims=[self.CALIBRATION_TIME],
                         attrs={'long_name': long_name, 'units': units})
        ds = xr.Dataset({a.name: a, b.name: b})
        return ds

    def calibrate_winds(self, sonics: IsfsDataset, eb: xr.DataArray,
                        begin: np.datetime64, end: np.datetime64):
        """
        Using the sonic wind component variables from @p sonics and the
        hotfilm voltage variable @p eb, calibrate the voltages with the wind
        speeds.
        """
        u, _, w = sonics.get_wind_variables(eb)
        u = u.sel(**{u.dims[0]: slice(begin, end)})
        w = w.sel(**{w.dims[0]: slice(begin, end)})
        spd = sonics.get_speed(u, w)
        return self.calibrate(spd, eb, begin, end)

    def calibrate(self, spd: xr.DataArray, eb: xr.DataArray,
                  begin: np.datetime64, end: np.datetime64):
        """
        Given the hotfilm bridge voltage data and a time period, compute a
        calibration by fitting the voltage to the corresponding sonic wind
        speed.
        """
        logger.debug("\neb=%s", eb)
        logger.debug("calibrating from %s to %s", begin, end)
        spd = resample_mean(spd, self.mean_interval)
        eb = eb.sel(**{eb.dims[0]: slice(begin, end)})
        eb = resample_mean(eb, self.mean_interval)
        self.begin, self.end = begin, end
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
        if len(spd) < 2 or len(eb) < 2:
            raise Exception("too few data points to calibrate")
        elif len(spd) != len(eb):
            raise Exception(f"num spd points {len(spd)} does not "
                            f"equal num eb points {len(eb)}")
        self.eb = eb
        self.spd = spd
        pfit = Polynomial.fit(spd**0.45, eb**2, 1)
        logger.debug("polynomial fit: %s, window=%s, domain=%s",
                     pfit, pfit.window, pfit.domain)
        pfit = pfit.convert()
        self.a, self.b = pfit.coef[0:2]
        logger.debug("polynomial converted: a=%.2f, b=%.2f, %s, "
                     "window=%s, domain=%s",
                     self.a, self.b, pfit, pfit.window, pfit.domain)
        return self

    def num_points(self):
        "Return the number of points used in this calibration."
        return len(self.eb)

    def speed(self, eb):
        """
        Given an array of hotfilm bridge voltages, compute the corresponding
        wind speeds using the stored coefficients of the least squares fit.
        """
        return hotfilm_voltage_to_speed(eb, self.a, self.b)

    def convert_to_wind_speed(self, eb: xr.DataArray) -> xr.DataArray:
        """
        Return a DataArray for the voltages converted to wind speeds.
        """
        eb = eb.sel(**{eb.dims[0]: slice(self.begin, self.end)})
        spd = self.speed(eb)
        spd.name = 'spdhf_%(height)s_%(site)s' % eb.attrs
        long_name = "hotfilm wind speed in vertical plane orthogonal to hotfilm"
        spd.attrs['long_name'] = long_name
        spd.attrs['units'] = "m/s"
        spd.attrs['site'] = eb.attrs['site']
        spd.attrs['height'] = eb.attrs['height']
        return spd

    def plot(self, ax: matplotlib.axes.Axes):
        """
        Plot the calibration curve on the given axes.
        """
        logger.debug("plotting calibration curve: eb=%s, spd=%s",
                     self.eb, self.spd)
        logger.debug("a=%s, b=%s", self.a, self.b)
        ebmin = self.eb.min().data
        ebmax = self.eb.max().data
        logger.debug("min eb=%s, max eb=%s", ebmin, ebmax)
        eb = np.linspace(ebmin, ebmax, 100)
        spd = self.speed(eb)
        label = f'Fit: Spd^0.45 = (eb^2 - {self.a:.2f})/{self.b:.2f})'
        # plot the calibration curve
        ax.plot(spd, eb, color='red', label=label)
        # plot the data
        ax.scatter(self.spd, self.eb)
        ax.set_xlabel(f"{self.spd.attrs['long_name']}")
        ax.set_ylabel(f"{self.eb.attrs['long_name']}")
        dtime = self.eb.coords[self.eb.dims[0]]
        first = dtime.data[0]
        ax.set_title(f"{dt_string(first)}")
        ax.legend()


class HotfilmDataset:

    HEIGHTS = {'ch0': '0.5m', 'ch1': '1m', 'ch2': '2m', 'ch3': '4m'}

    def __init__(self):
        self.dataset = None
        self.timev = None
        self.timed = None

    def open(self, filename):
        self.dataset = xr.open_dataset(filename)
        self.timev = self.dataset['time']
        self.timed = self.timev.dims[0]
        logging.debug(f"Opened hotfilm dataset: {filename}, %s...%s",
                      self.timev[0], self.timev[-1])
        return self

    def get_variable(self, name: str) -> xr.DataArray:
        eb = self.dataset[name]
        # this should have been in the dataset, so hardcode it until it is
        if 'long_name' not in eb.attrs:
            eb.attrs['long_name'] = f'{eb.name} bridge voltage (V)'
        if 'site' not in eb.attrs:
            eb.attrs['site'] = 't0'
        if 'height' not in eb.attrs:
            eb.attrs['height'] = self.HEIGHTS[eb.name]
        return eb

    def close(self):
        self.dataset.close()


class HotfilmWindSpeedDataset:
    """
    Wrapper for a xarray.Dataset which contains hotfilm wind speeds calibrated
    from hotfilm bridge voltages.
    """

    def __init__(self):
        """
        Create HotfilmWindSpeedDataset with the given time coordinates.
        """
        self.dataset = xr.Dataset()

    def add_wind_speed(self, spd: xr.DataArray):
        """
        Merge a hotfilm wind speed variable into the dataset.
        """
        self.dataset = self.dataset.merge(spd)
        logger.debug("merged wind speed variable:\n%sresult:\n%s",
                     spd, self.dataset)

    def add_calibration(self, calibration: xr.Dataset):
        """
        Add a calibration to the dataset.
        """
        self.dataset = self.dataset.merge(calibration)
        return self.dataset

    def open(self, filename):
        self.dataset = xr.open_dataset(filename)
        self.timev = self.dataset['time']
        self.timed = self.timev.dims[0]
        logging.debug(f"Opened hotfilm speed dataset: {filename}, %s...%s",
                      self.timev[0], self.timev[-1])
        return self

    def save(self, fspec: str):
        """
        Save the current Dataset with a filename containing the start time.
        """
        if self.dataset is None:
            raise Exception("no dataset to save")
        outpath = OutputPath()
        logger.debug("saving dataset:\n%s", self.dataset)
        try:
            filename = outpath.start(fspec, self.dataset)
            ds = convert_time_coordinate(self.dataset, self.dataset.time)
            cdim = ds.coords[HotfilmCalibration.CALIBRATION_TIME]
            ds = convert_time_coordinate(ds, cdim)
            logger.debug("calling to_netcdf() on dataset:\n%s", ds)
            ds.to_netcdf(filename)
            filename = outpath.finish()
            logging.info(f"Saved hotfilm wind speed dataset: {filename}")
        finally:
            outpath.remove()
