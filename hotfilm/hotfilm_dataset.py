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
import matplotlib.pyplot as plt
import matplotlib.axes


logger = logging.getLogger(__name__)


def _dt_string(dt: np.datetime64) -> str:
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

    def calibrate(self, spd: xr.DataArray, eb: xr.DataArray,
                  begin: np.datetime64, end: np.datetime64):
        """
        """
        spd = spd.sel(**{spd.dims[0]: slice(begin, end)})
        spd = resample_mean(spd, self.mean_interval)
        eb = eb.sel(**{eb.dims[0]: slice(begin, end)})
        eb = resample_mean(eb, self.mean_interval)
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

    def plot(self, ax: matplotlib.axes.Axes):
        """
        Plot the calibration curve on the given axes.
        """
        logger.debug("plotting calibration curve: eb=%s, spd=%s",
                     self.eb, self.spd)
        logger.debug("a=%s, b=%s", self.a, self.b)
        ebmin = self.eb.min().item()
        ebmax = self.eb.max().item()
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
        last = dtime.data[-1]
        ax.set_title(f"{_dt_string(first)} to {_dt_string(last)}")
        ax.legend()


class HotfilmDataset:

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

    def create_calibration(self, spd: xr.DataArray,
                           begin: np.datetime64, end: np.datetime64,
                           mean_interval: np.timedelta64):
        """
        Given a DataArray of wind speeds, such as a sonic anemometer wind
        speed variable from an ISFS dataset, compute mean voltages and speeds
        over the given time period and use them to create a
        HotfilmCalibration.  Return the calibration.
        """
        spd.resample(time='5min').mean()

    def close(self):
        self.dataset.close()


import sys
from isfs_dataset import IsfsDataset, rdatetime


def resample_mean(da: xr.DataArray, period: str) -> xr.DataArray:
    """
    Resample the given DataArray to the mean over the given period, assuming
    only that time is the first dimension but not necessarily named 'time'.
    """
    indexer = {da.dims[0]: period}
    return da.resample(**indexer).mean(skipna=True, keep_attrs=True)


if __name__ == "__main__":
    logger.setLevel(logging.DEBUG)
    filename = sys.argv[1]
    films = HotfilmDataset().open(filename)
    sonics = IsfsDataset().open(sys.argv[2])
    logger.debug("\nhotfilm.timev=%s", films.timev)
    calperiod = np.timedelta64(300, 's')
    # start with first time in hotfilm dataset rounded to cal period.
    first = films.timev.data[0]
    last = films.timev.data[-1]
    logger.debug("first=%s, type=%s", first, type(first))
    begin = rdatetime(first, calperiod)
    end = rdatetime(last, calperiod)
    u = sonics.get_variable("u_0_5m_t0")
    w = sonics.get_variable("w_0_5m_t0")
    spd = np.sqrt(u**2 + w**2)
    uname = u.attrs['short_name']
    wname = w.attrs['short_name']
    spd.attrs['long_name'] = f'|({uname},{wname})| (m/s)'
    logger.debug("\nspd=%s", spd)
    eb = films.dataset['ch0']
    # this should have been in the dataset, so hardcode it until it is
    eb.attrs['long_name'] = f'{eb.name} bridge voltage (V)'
    logger.debug("\neb=%s", eb)
    cals = []
    # Panel of calibration plots
    nrows, ncols = 2, 3
    fig, axs = plt.subplots(nrows, ncols)
    nplots = nrows * ncols

    def subplot(iplot):
        if nrows * ncols == 1:
            return axs
        return axs[iplot // ncols, iplot % ncols]

    title = f'Calibrations from {_dt_string(begin)} to {_dt_string(end)}'
    iplot = 0
    while begin < end and iplot < nplots:
        next_time = begin + calperiod
        logger.debug("calibrating from %s to %s", begin, next_time)
        # select voltage and wind speeds
        # use open-ended slice for next_time
        end_slice = next_time - np.timedelta64(1, 'ns')
        try:
            hfc = HotfilmCalibration().calibrate(spd, eb, begin, end_slice)
            hfc.plot(subplot(iplot))
            iplot += 1
            cals.append(hfc)
        except Exception as e:
            logger.error(f"calibration failed: {e}")
        begin = next_time
    films.close()
    fig.suptitle(title, fontsize=16)
    plt.show()
