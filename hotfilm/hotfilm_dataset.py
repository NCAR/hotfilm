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
from .utils import set_time_coordinate_units


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

    def __init__(self):
        self.name = None
        self.eb = None
        self.spd = None
        self._num_points = None
        self.a = None
        self.b = None
        self.mean_interval_seconds = 1
        self.period_seconds = 300
        self.begin = None

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
        u, w = sonics.get_wind_data(eb, 'uw', begin, end)
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
        return means.rename({f'{da.dims[0]}': f'time_mean_{period}'})

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
        self.spd = spd
        pfit = Polynomial.fit(spd**0.45, eb**2, 1)
        logger.debug("polynomial fit: %s, window=%s, domain=%s",
                     pfit, pfit.window, pfit.domain)
        pfit = pfit.convert()
        self.a, self.b = pfit.coef[0:2]
        self._num_points = len(eb)
        logger.debug("polynomial converted: a=%.2f, b=%.2f, %s, "
                     "window=%s, domain=%s",
                     self.a, self.b, pfit, pfit.window, pfit.domain)
        return self

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
        # get the speed variable and convert it back to voltages
        spd = self.spd
        eb = self.eb
        logger.debug("plotting calibration curve:\n-->eb=%s\n-->spd=%s",
                     eb, spd)
        logger.debug("a=%s, b=%s", self.a, self.b)
        ebmin = eb.min().data
        ebmax = eb.max().data
        logger.debug("min eb=%s, max eb=%s", ebmin, ebmax)
        ebline = np.linspace(ebmin, ebmax, 100)
        spdline = self.speed(ebline)
        label = f'Fit: Spd^0.45 = (eb^2 - {self.a:.2f})/{self.b:.2f})'
        # plot the calibration curve
        ax.plot(spdline, ebline, color='red', label=label)
        # plot the data
        ax.scatter(spd, eb)
        ax.set_xlabel(f"{spd.name} ({spd.attrs['units']})")
        ax.set_ylabel(f"{eb.name} ({eb.attrs['units']})")
        dtime = eb.coords[eb.dims[0]]
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
        logger.info(f"opening hotfilm dataset: {filename}")
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
            eb.attrs['long_name'] = f'{eb.name} bridge voltage'
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
    CALIBRATION_TIME = 'time_calibration'

    def __init__(self):
        """
        Create HotfilmWindSpeedDataset with the given time coordinates.
        """
        self.dataset = xr.Dataset()

    def add_calibration(self, hfc: HotfilmCalibration):
        """
        Create a Dataset with variables for the coefficients and a time
        coordinate, and add it to this Dataset.
        """
        ds = xr.Dataset()
        name = hfc.get_name()
        attrs = {'long_name': 'Calibration period begin time',
                 'period_seconds': hfc.period_seconds,
                 'mean_interval_seconds': hfc.mean_interval_seconds}
        timed = xr.DataArray(name=self.CALIBRATION_TIME,
                             data=[hfc.begin],
                             dims=[self.CALIBRATION_TIME],
                             attrs=attrs)

        long_name = "first-degree coefficient b: eb^2=a+b*spd^0.45"
        units = "V^2"
        a = xr.DataArray(name=f'a_{name}', data=[hfc.a],
                         dims=[self.CALIBRATION_TIME],
                         coords={timed.name: timed},
                         attrs={'long_name': long_name, 'units': units})
        long_name = "zero-degree coefficient a: eb^2=a+b*spd^0.45"
        units = "(V^2)(m/s)^-0.45"
        b = xr.DataArray(name=f'b_{name}', data=[hfc.b],
                         coords={timed.name: timed},
                         dims=[self.CALIBRATION_TIME],
                         attrs={'long_name': long_name, 'units': units})

        # include the eb and spd mean data as variables, with yet another time
        # dimension.
        ds = xr.Dataset({a.name: a, b.name: b,
                         hfc.eb.name: hfc.eb, hfc.spd.name: hfc.spd})
        self.dataset = self.dataset.merge(ds)

    def add_wind_speed(self, hfc: HotfilmCalibration, eb: xr.DataArray):
        """
        Use HotfilmCalibration @p hfc to convert a DataArray @p eb of voltages
        to wind speed and add the wind speed variable to this Dataset.
        """
        begin = hfc.begin
        end = hfc.get_end_time(begin)
        eb = eb.sel(**{eb.dims[0]: slice(begin, end)})
        spd = hfc.speed(eb)
        spd.name = 'spdhf_%(height)s_%(site)s' % eb.attrs
        long_name = "wind speed orthogonal to hotfilm"
        spd.attrs['long_name'] = long_name
        spd.attrs['units'] = "m/s"
        spd.attrs['site'] = eb.attrs['site']
        spd.attrs['height'] = eb.attrs['height']
        spd.attrs['hotfilm_channel'] = eb.name
        self.add_calibration(hfc)
        self.dataset = self.dataset.merge(spd)
        logger.debug("merged wind speed variable:\n%sresult:\n%s",
                     spd, self.dataset)

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
            tfile = outpath.start(fspec, self.dataset)
            ds = convert_time_coordinate(self.dataset, self.dataset.time)
            cdim = ds.coords[self.CALIBRATION_TIME]
            # microsecond resolution is not needed for calibration time
            # coordinates, but if we don't set it explicitly then xarray will
            # use minutes, and we'd like the units to be consistent regardless
            # of the calibration period.
            set_time_coordinate_units(cdim, 'seconds')
            cdim = ds.coords[[dim for dim in ds.dims if 'mean' in dim][0]]
            set_time_coordinate_units(cdim, 'seconds')

            logger.debug("calling to_netcdf() on dataset:\n%s", ds)
            # filename must be passed as a string and not Path, despite the
            # type hint for to_netcdf() that accepts PathLike, otherwise a
            # test for a file path inside xarray.backends.api.to_netcdf()
            # fails and forces the engine to be scipy.
            ds.to_netcdf(tfile.name, engine="netcdf4", format='NETCDF4')
            filename = outpath.finish()
            logging.info(f"saved hotfilm wind speed dataset: {filename}")
        finally:
            outpath.remove()

    def get_calibration_times(self) -> xr.DataArray:
        """
        Return the time coordinate of the calibrations in the dataset.
        """
        if self.dataset is None:
            raise Exception("no dataset to get calibration times")
        cdim = self.dataset[self.CALIBRATION_TIME]
        return cdim

    def get_speed_variables(self):
        """
        Return the wind speed variables in this dataset.
        """
        # the wind speeds are all the variables which start spdhf
        return [self.dataset[name] for name in self.dataset.data_vars
                if name.startswith('spdhf')]

    def get_calibration(self, begin: np.datetime64,
                        spd: xr.DataArray) -> HotfilmCalibration:
        """
        Given a wind speed variable, return the calibration for it.
        """
        name = spd.attrs['hotfilm_channel']
        logger.debug("getting calibration for %s", name)
        hfc = HotfilmCalibration()
        hfc.name = name
        loc = {self.CALIBRATION_TIME: begin}
        hfc.a = self.dataset[f'a_{name}'].sel(**loc).data
        hfc.b = self.dataset[f'b_{name}'].sel(**loc).data

        # calibration parameters are attached to time coordinate attributes
        ctime = self.dataset[self.CALIBRATION_TIME]
        hfc.period_seconds = ctime.attrs['period_seconds']
        hfc.mean_interval_seconds = ctime.attrs['mean_interval_seconds']
        hfc.begin = begin
        return hfc
