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
from scipy.optimize import curve_fit
import matplotlib.pyplot as plt
import matplotlib.axes


def hotfilm_voltage_to_speed(eb, a, b):
    """
    Given the relationship between hotfilm bridge voltage and wind speed:

        eb = coef[1] + coef[2] * spd^0.45

    Compute the wind speed from Eb with the coefficients a and b:

        spd = ((eb^2 - a)/b)^(1/0.45)
    """
    spd = ((eb**2 - a)/b)**(1/0.45)
    return spd


class HotfilmCalibration:
    """
    Create a hot film calibration and manage metadata for it.
    """
    a: float
    b: float
    pcov: np.ndarray
    eb: xr.DataArray
    spd: xr.DataArray

    def __init__(self):
        self.a = None
        self.b = None
        self.pcov = None
        self.eb = None
        self.spd = None

    def fit(self, spd: xr.DataArray, eb: xr.DataArray):
        """
        Given an array of hotfilm bridge voltages and a corresponding array of
        wind speeds, compute the coefficients of a least squares fit to the
        hotfilm_voltage_to_speed() function and store them in this object.
        """
        self.eb = eb
        self.spd = spd
        popt, pcov = curve_fit(hotfilm_voltage_to_speed, eb, spd)
        self.a, self.b = popt
        self.pcov = pcov
        return self

    def speed(self, eb):
        """
        Given an array of hotfilm bridge voltages, compute the corresponding
        wind speeds using the coefficients of the least squares fit.
        """
        return hotfilm_voltage_to_speed(eb, self.a, self.b)

    def plot(self, ax: matplotlib.axes.Axes):
        """
        Plot the calibration curve on the given axes.
        """
        eb = np.linspace(self.eb.min(), self.eb.max(), 100)
        spd = self.speed(eb)
        label = f'Fit: Eb = ((Spd^2 - {self.a:.2f})/{self.b:.2f})^(1/0.45)'
        ax.plot(spd, eb, color='red', label=label)
        ax.scatter(self.spd, self.eb)
        ax.set_xlabel(f"{self.spd.attrs['long_name']}")
        ax.set_ylabel(f"{self.eb.attrs['long_name']}")
        title = (f"{self.eb.attrs['long_name']} vs "
                 f"{self.spd.attrs['long_name']}")
        ax.set_title(title)
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
