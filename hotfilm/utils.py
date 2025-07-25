
import subprocess as sp
from pathlib import Path
import logging
import numpy as np
import xarray as xr
import pandas as pd
from . import VERSION

logger = logging.getLogger(__name__)

_seconds_per_day = 24*60*60
_microseconds_per_second = 1000000


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


def dt_string(dt: np.datetime64) -> str:
    return np.datetime_as_string(dt, unit='s')


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


def get_git_describe() -> str | None:
    """
    Get the git describe string for the current commit to provide extra
    context for the explicit version string, especially during development.
    If no .git directory is found, return None.
    """
    gd = None
    source = Path(__file__).absolute().resolve().parent.parent
    if (source / '.git').exists():
        gd = sp.check_output(['git', 'describe', '--always'],
                             universal_newlines=True,
                             cwd=str(source)).strip()
    return gd


def add_history_to_dataset(ds: xr.Dataset, script: str,
                           command_line: str) -> None:
    """
    Add history and related attributes to the dataset with the given command
    line, and qualified with the script name.
    """
    timestamp = pd.Timestamp.utcnow().strftime('%Y-%m-%dT%H:%M:%SZ')
    ds.attrs['history'] = f'generated by {script} on {timestamp}'
    if command_line:
        ds.attrs[f'{script}_command_line'] = command_line
    gd = get_git_describe()
    gd = f' ({gd})' if gd else ''
    ds.attrs[f'{script}_version'] = f'{VERSION}{gd}'


def r_squared(actual: xr.DataArray, predicted: xr.DataArray) -> float:
    """
    Calculate the R-squared value between the actual and predicted values.
    There is some suggestion on the web that R-squared is not appropriate
    for nonlinear regressions, since the total sum of squares is the
    difference from the mean, even though the fit may not be linear.  Maybe
    the standard error of the regression would be safer.  (I think that is
    the same as mean absolute error.)  However, the fit used by hotfilms is
    in fact linear, except the variable terms are exponentials of the
    measured variables, Eb^2 and Spd^0.45.

    https://en.wikipedia.org/wiki/Coefficient_of_determination
    https://blog.minitab.com/en/adventures-in-statistics-2/why-is-there-no-r-squared-for-nonlinear-regression
    https://blog.minitab.com/en/adventures-in-statistics-2/regression-analysis-how-to-interpret-s-the-standard-error-of-the-regression
    """
    if actual.size != predicted.size:
        raise ValueError("actual and predicted must have the same size")

    actual_mean = np.mean(actual)
    ss_total = np.sum((actual - actual_mean) ** 2)
    ss_residual = np.sum((actual - predicted) ** 2)
    logger.debug("r_squared(): mean_actual=%f, ss_total=%f, ss_residual=%f",
                 actual_mean, ss_total, ss_residual)
    logger.debug("actual: \n%s", actual)
    logger.debug("predicted: \n%s", predicted)

    rsquared = 0
    # Force R^2 into interval [0, 1]
    if ss_total > 0 and ss_residual < ss_total:
        rsquared = 1 - (ss_residual / ss_total)
    logger.debug("R-squared: %f", rsquared)
    return rsquared
