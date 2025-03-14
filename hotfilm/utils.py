
import logging
import numpy as np
import xarray as xr
import pandas as pd

logger = logging.getLogger(__name__)

_microseconds_per_seconds = 1000000
_microseconds_per_day = 24*60*60*_microseconds_per_seconds


def td_to_microseconds(td64: np.timedelta64) -> int:
    td = pd.to_timedelta(td64)
    return (td.days * _microseconds_per_day +
            td.seconds * _microseconds_per_seconds +
            td.microseconds)


def convert_time_coordinate(ds: xr.Dataset, dt: xr.DataArray) -> xr.Dataset:
    """
    Convert a time coordinate in a Dataset to int64 microseconds since the
    first time in the coordinate array.
    """
    # numerous and varied attempts failed to get xarray to encode
    # the time as microseconds since base, so do it manually.
    when = pd.to_datetime(dt.data[0])
    base = when.replace(microsecond=0)
    units = ('microseconds since %s' %
             base.strftime("%Y-%m-%d %H:%M:%S+00:00"))
    base = np.datetime64(base)
    vtime = np.array([td_to_microseconds(t) for t in (dt - base).data],
                     dtype='int64')
    ds = ds.assign_coords({dt.name: vtime})
    ds[dt.name].attrs.update(dt.attrs)
    ds[dt.name].attrs['units'] = units
    ds[dt.name].encoding = {'dtype': 'int64'}
    logger.debug("converted time coordinate:\n%s\n -->to-->\n%s",
                 dt, ds[dt.name])
    return ds


def set_time_coordinate_units(cdim: xr.DataArray, units: str) -> None:
    """"
    Set the encoding for this time coordinate relative to the first time
    using @p units.
    """
    base = pd.to_datetime(cdim.data[0])
    cdim.encoding = {'units': f'{units} since %s' %
                              base.strftime("%Y-%m-%d %H:%M:%S+00:00")}
