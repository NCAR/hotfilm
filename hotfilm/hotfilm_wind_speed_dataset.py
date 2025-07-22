import logging
import xarray as xr
import numpy as np
from hotfilm.outout_path import OutputPath
from hotfilm.hotfilm_calibration import HotfilmCalibration
from hotfilm.utils import dt_string
from .utils import convert_time_coordinate
from .utils import set_time_coordinate_units


logger = logging.getLogger(__name__)


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

    def create_calibration_coordinate(self, hfc: HotfilmCalibration):
        """
        Return a time coordinate for the calibration period.
        """
        attrs = {'long_name': 'Calibration period begin time',
                 'period_seconds': np.int32(hfc.period_seconds),
                 'mean_interval_seconds': np.int32(hfc.mean_interval_seconds)}
        timed = xr.DataArray(name=self.CALIBRATION_TIME,
                             data=[hfc.begin],
                             dims=[self.CALIBRATION_TIME],
                             attrs=attrs)
        return timed

    def add_calibration(self, hfc: HotfilmCalibration):
        """
        Create a Dataset with variables for the coefficients and a time
        coordinate, and add it to this Dataset.
        """
        ds = xr.Dataset()
        name = hfc.get_name()
        timed = self.create_calibration_coordinate(hfc)

        long_name = "first-degree coefficient b: eb^2=a+b*spd^0.45"
        units = "V^2"
        a = xr.DataArray(name=f'a_{name}', data=[hfc.a],
                         coords={timed.name: timed},
                         attrs={'long_name': long_name, 'units': units})
        a.encoding['dtype'] = 'float32'
        long_name = "zero-degree coefficient a: eb^2=a+b*spd^0.45"
        units = "(V^2)(m/s)^-0.45"
        b = xr.DataArray(name=f'b_{name}', data=[hfc.b],
                         coords={timed.name: timed},
                         attrs={'long_name': long_name, 'units': units})
        b.encoding['dtype'] = 'float32'

        long_name = "number of points in calibration"
        units = "1"
        npoints = xr.DataArray(name=f'npoints_{name}',
                               data=[hfc.num_points()],
                               coords={timed.name: timed},
                               attrs={'long_name': long_name, 'units': units})
        npoints.encoding['dtype'] = 'float32'

        long_name = "RMS difference between calibration and sonic wind speed"
        units = "m/s"
        rms = xr.DataArray(name=f'rms_{name}',
                           data=[hfc.rms],
                           coords={timed.name: timed},
                           attrs={'long_name': long_name, 'units': units})
        rms.encoding['dtype'] = 'float32'

        long_name = "R^2 coefficient of determination between calibration fit and sonic wind speed"
        units = "1"
        rsquared = xr.DataArray(name=f'r2_{name}',
                                data=[hfc.rsquared_speed],
                                coords={timed.name: timed},
                                attrs={'long_name': long_name, 'units': units})
        rsquared.encoding['dtype'] = 'float32'

        # include the eb and spd mean data as variables, with yet another time
        # dimension.
        ds = xr.Dataset({a.name: a, b.name: b,
                         hfc.u.name: hfc.u,
                         hfc.v.name: hfc.v,
                         hfc.w.name: hfc.w,
                         npoints.name: npoints,
                         rms.name: rms,
                         rsquared.name: rsquared,
                         hfc.eb.name: hfc.eb,
                         hfc.spd_sonic.name: hfc.spd_sonic})
        self.dataset = self.dataset.merge(ds, combine_attrs='identical')

    def add_wind_speed(self, hfc: HotfilmCalibration, eb: xr.DataArray):
        """
        Use HotfilmCalibration @p hfc to convert a DataArray @p eb of voltages
        to wind speed and add the wind speed variable to this Dataset.
        """
        begin = hfc.begin
        end = hfc.get_end_time(begin)
        eb = eb.sel(**{eb.dims[0]: slice(begin, end)})
        spd = hfc.speed(eb)
        # follow isfs naming convention which replaces . with underscore,
        # so 0.5m height is inserted into name as 0_5m
        name = ('spdhf_%(height)s_%(site)s' % eb.attrs).replace('.', '_')
        spd.name = name
        long_name = "wind speed orthogonal to hotfilm"
        spd.attrs['long_name'] = long_name
        spd.attrs['units'] = "m/s"
        spd.attrs['site'] = eb.attrs['site']
        spd.attrs['height'] = eb.attrs['height']
        spd.attrs['hotfilm_channel'] = eb.name
        # in case source dataset has older double type
        spd.encoding['dtype'] = 'float32'
        self.add_calibration(hfc)
        self.dataset = self.dataset.merge(spd, combine_attrs='identical')
        logger.debug("merged wind speed variable:\n%sresult:\n%s",
                     spd, self.dataset)

    def open(self, filename):
        self.dataset = xr.open_dataset(filename)
        timev = self.dataset['time']
        ctime = self.dataset[self.CALIBRATION_TIME]
        mtime = next(v for v in self.dataset.coords.values()
                     if 'mean' in v.name)
        logging.info(f"opened hotfilm speed dataset: {filename}, "
                     "speeds=>%s...%s, cals=>%s...%s, means=>%s...%s",
                     dt_string(timev[0]), dt_string(timev[-1]),
                     dt_string(ctime[0]), dt_string(ctime[-1]),
                     dt_string(mtime[0]), dt_string(mtime[-1]))
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
            # despite adding encoding attributes to all the variables in the
            # Dataset, apparently that gets dropped by a merge, so this just
            # enforces the desired encoding when saving to netcdf.
            encodings = {
                var.name: {'dtype': 'float32'}
                for var in ds.data_vars.values()
            }
            # filename must be passed as a string and not Path, despite the
            # type hint for to_netcdf() that accepts PathLike, otherwise a
            # test for a file path inside xarray.backends.api.to_netcdf()
            # fails and forces the engine to be scipy.
            ds.to_netcdf(tfile.name, engine="netcdf4", format='NETCDF4',
                         encoding=encodings)
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
        return [v for v in self.dataset.data_vars.values()
                if v.name.startswith('spdhf')]

    def get_calibration(self, begin: np.datetime64,
                        spd: xr.DataArray) -> HotfilmCalibration:
        """
        Given a wind speed variable, return the calibration for it.  The
        calibration variables are found by matching the height and site of the
        given speed variable.
        """
        name = spd.attrs['hotfilm_channel']
        site = spd.attrs['site']
        height = spd.attrs['height']
        logger.debug("getting calibration for %s, channel %s, at %s",
                     spd.name, name, dt_string(begin))
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

        # get slices for the mean voltages and speeds
        height_ = height.replace('.', '_')
        vars = [v for v in self.dataset.data_vars.values() if
                v.attrs.get('site') == site and
                v.attrs.get('height') in [height, height_]]
        eb = next(v for v in vars if v.name.startswith('ch'))
        spd = next(v for v in vars if v.name.startswith('spd_'))
        end = hfc.get_end_time(begin)
        calslice = {eb.dims[0]: slice(begin, end)}
        eb = eb.sel(**calslice)
        spd = spd.sel(**calslice)
        hfc.eb = eb
        hfc.spd_sonic = spd
        if f'npoints_{name}' not in self.dataset.data_vars:
            hfc._num_points = len(eb)
        else:
            hfc._num_points = self.dataset[f'npoints_{name}'].sel(**loc).data
        if f'rms_{name}' not in self.dataset.data_vars:
            hfc.calculate_rms()
        else:
            hfc.rms = self.dataset[f'rms_{name}'].sel(**loc).data
        logger.debug("%d calibration points: eb=%s, spd=%s, "
                     "period_seconds=%s, mean_interval_seconds=%s, "
                     "npoints=%s, rms=%s",
                     hfc.num_points(), eb.name, spd.name,
                     hfc.period_seconds, hfc.mean_interval_seconds,
                     hfc._num_points, hfc.rms)
        return hfc
