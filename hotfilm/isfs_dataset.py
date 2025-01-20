"""
Read wind vectors from ISFS netcdf files.
"""

import logging
import numpy as np
import xarray as xr
import datetime as dt

logger = logging.getLogger(__name__)


def rdatetime(when: np.datetime64, period: np.timedelta64) -> np.datetime64:
    "Round when to the nearest multiple of period."
    when_ns = when.astype('datetime64[ns]')
    period_ns = period.astype('timedelta64[ns]').astype(int)
    mod = when_ns.astype(int) % period_ns
    # compare with zero since period_ns // 2 is zero when period_ns is 1
    if mod < period_ns // 2 or mod == 0:
        when_ns -= mod
    else:
        when_ns += period_ns - mod
    return np.datetime64(when_ns, 'ns').astype(when.dtype)


class IsfsDataset:

    def __init__(self):
        self.dataset = None
        self.timev = None
        self.timed = None

    def open(self, filename):
        self.dataset = xr.open_dataset(filename)
        timev = self.dataset['time']
        # for some reason xarray only parses the date in the time units, so
        # the time of day needs to be added.
        timespec = 'seconds since %Y-%m-%d %H:%M:%S 00:00'
        units = timev.encoding['units']
        tod = dt.datetime.strptime(units, timespec)
        seconds = tod.hour * 3600 + tod.minute * 60 + tod.second
        timev = timev + np.timedelta64(seconds, 's')
        self.dataset['time'] = timev
        self.timev = timev
        self.timed = self.timev.dims[0]
        logger.debug(f"Opened dataset: {filename}, %s...%s",
                     self.timev[0], self.timev[-1])
        return self

    def resample_dim_name(self, dsv: xr.DataArray):
        "Derive the resampled time dimension name."
        dims = dsv.dims
        if len(dims) == 1:
            return dims[0]
        nsample = self.dataset.sizes[dims[1]]
        dname = f"time{nsample}"
        return dname

    def interpolate_times(self, dsv: xr.DataArray) -> xr.DataArray:
        """
        Make sure the dataset has a time coordinate for these dimemsions, then
        return it.
        """
        # find the unique name for the time coordinate to see if it exists
        dname = self.resample_dim_name(dsv)
        if dname in self.dataset.coords:
            return self.dataset.coords[dname]
        dims = dsv.dims
        if len(dims) == 1:
            return self.timev
        ntime = self.dataset.sizes[dims[0]]
        nsample = self.dataset.sizes[dims[1]]
        period = np.timedelta64(1000000000 // nsample, 'ns')
        logger.debug(f"calculating 1D time coordinate for {dims}: "
                     f"{nsample} samples, period {period} ...")
        tra = np.ndarray((ntime, nsample), dtype='datetime64[ns]')
        for i in range(ntime):
            seconds = self.timev.values[i] - np.timedelta64(500, 'ms')
            for j in range(nsample):
                tra[i, j] = seconds + (j * period)
        sampled = xr.DataArray(name=dname, data=tra, dims=dims)
        self.dataset.coords[dname] = sampled
        logger.debug(f"Interpolated time coordinate for {dims}: %s", sampled)
        return sampled

    def get_variable(self, variable):
        """
        Return the named variable from the dataset as a 1D time series.  For
        variables with rates higher that 1 Hz, this means flattening the time
        dimensions and interpolating the sub-second timestamps.
        """
        dsv: xr.DataArray
        dsv = self.dataset[variable]
        self.interpolate_times(dsv)
        dsv = self.reshape_variable(dsv)
        return dsv

    def reshape_variable(self, dsv: xr.DataArray):
        """
        Given a time series with a sub-sample dimension, reshape it to a 1D
        time series.  If it is already 1D, return it unchanged.
        """
        if len(dsv.dims) == 1:
            return dsv
        data = dsv.values.flatten()
        dname = self.resample_dim_name(dsv)
        dtimes = self.interpolate_times(dsv)
        times = dtimes.values.flatten()
        reshaped = xr.DataArray(name=dsv.name, data=data,
                                coords={dname: times},
                                attrs=dsv.attrs)
        logger.debug("reshaped %s: %s", dsv.name, reshaped)
        return reshaped

    def close(self):
        self.dataset.close()
