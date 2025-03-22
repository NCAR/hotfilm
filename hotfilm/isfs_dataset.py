"""
Read wind vectors from ISFS netcdf files.
"""

from pathlib import Path
import logging
import numpy as np
import xarray as xr
import datetime as dt
import pandas as pd


logger = logging.getLogger(__name__)


class IsfsDataset:

    DEFAULT_PATH_SPEC = "isfs_m2hats_qc_hr_inst_%Y%m%d_%H0000.nc"

    def __init__(self, pathspec: str = None):
        self.dataset = None
        self.timev = None
        self.timed = None
        self.pathspec = pathspec
        self.filename = None
        # as a convenience, if pathspec is a directory, then automatically
        # append the filename spec
        if pathspec is None:
            self.pathspec = self.DEFAULT_PATH_SPEC
        elif Path(pathspec).is_dir():
            self.pathspec = str(Path(pathspec) / self.DEFAULT_PATH_SPEC)

    def lookup_filepath(self, when: np.datetime64) -> str:
        """
        Return the filename for the given time by formatting the pathspec with
        @p when.
        """
        dt = pd.to_datetime(when)
        filepath = Path(dt.strftime(str(self.pathspec)))
        if not filepath.exists():
            logger.error("File for time %s does not exist: %s",
                         dt, filepath)
            return None
        return filepath

    def load_filepath(self, when: np.datetime64):
        """
        Make sure the dataset for the given time is loaded, either because
        it's the current file or by closing the current file and opening the
        new one.
        """
        filepath = self.lookup_filepath(when)
        if filepath != self.filename:
            self.close()
            self.open(filepath)

    def open(self, filename):
        self.filename = filename
        logger.info(f"loading isfs dataset: {filename}")
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

    def get_wind_variables(self, variable: xr.DataArray,
                           components: list) -> tuple[xr.DataArray]:
        """
        Given a variable with a height and site attribute, return the sonic
        wind component variables for that height and for the components
        named in @p components: u, v, or w.
        """
        height = variable.attrs['height'].replace('.', '_')
        site = variable.attrs['site']
        return [self.get_variable(f'{c}_{height}_{site}') for c in components]

    def get_wind_data(self, variable: xr.DataArray,
                      components: list,
                      begin: np.datetime64,
                      end: np.datetime64) -> tuple[xr.DataArray]:
        """
        Find the requested wind variables for the specific time range,
        closing the current file and opening a new one as needed.
        """
        self.load_filepath(begin)
        vars = self.get_wind_variables(variable, components)
        return [v.sel(**{v.dims[0]: slice(begin, end)}) for v in vars]

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

    def get_speed(self, u: xr.DataArray, w: xr.DataArray) -> xr.DataArray:
        """
        Return speed DataArray as the norm of the two wind vector components.
        """
        spd = np.sqrt(u**2 + w**2)
        uname = u.attrs['short_name']
        wname = w.attrs['short_name']
        spd.attrs.update(u.attrs)
        spd.attrs['long_name'] = f'|({uname},{wname})|'
        name = f'spd_{spd.attrs["height"]}_{spd.attrs["site"]}'
        name = name.replace('.', '_')
        spd.name = name
        return spd

    def close(self):
        if self.dataset is None:
            return
        self.dataset.close()
        self.dataset = None
        self.timev = None
        self.timed = None
        # consider if memory not released soon enough
        # gc.collect()
