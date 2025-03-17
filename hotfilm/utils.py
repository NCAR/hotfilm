
import logging
import numpy as np
import xarray as xr
import pandas as pd

logger = logging.getLogger(__name__)

_seconds_per_day = 24*60*60
_microseconds_per_second = 1000000


def td_to_microseconds(td64: np.timedelta64) -> int:
    td = pd.to_timedelta(td64)
    return ((td.days * _seconds_per_day +
             td.seconds) * _microseconds_per_second +
            td.microseconds)


def td_to_seconds(td64: np.timedelta64) -> int:
    td = pd.to_timedelta(td64)
    return (td.days * _seconds_per_day) + td.seconds


def convert_time_coordinate(ds: xr.Dataset, dt: xr.DataArray,
                            ustep: str = "microseconds") -> xr.Dataset:
    """
    Convert a time coordinate in a Dataset to int64 microseconds or seconds
    since the first time in the coordinate array.  @p ustep is "seconds" or
    "microseconds".
    """
    # numerous and varied attempts failed to get xarray to encode
    # the time as microseconds since base, so do it manually.
    when = pd.to_datetime(dt.data[0])
    base = when.replace(microsecond=0)
    units = (f'{ustep} since %s' %
             base.strftime("%Y-%m-%d %H:%M:%S+00:00"))
    base = np.datetime64(base)
    convert = td_to_seconds
    if ustep == 'microseconds':
        convert = td_to_microseconds
    elif ustep != 'seconds':
        raise ValueError(f"unknown time unit: {ustep}")
    vtime = np.array([convert(t) for t in (dt - base).data],
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
