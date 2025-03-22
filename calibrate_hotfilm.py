#! /bin/env python
"""
Given a hotfilm voltage dataset and an ISFS dataset with sonic wind component
variables, calibrate the hotfilm voltages against sonic wind speed, then write
the computed wind speeds to netcdf, or plot the calibrations.
"""
import sys
import logging
import argparse

import matplotlib.pyplot as plt
import numpy as np
import xarray as xr
import pandas as pd

from hotfilm.isfs_dataset import IsfsDataset, rdatetime
from hotfilm.hotfilm_dataset import HotfilmDataset
from hotfilm.hotfilm_dataset import HotfilmWindSpeedDataset
from hotfilm.hotfilm_dataset import HotfilmCalibration
from hotfilm.hotfilm_dataset import dt_string
from hotfilm.utils import add_history_to_dataset

logger = logging.getLogger(__name__)


class CalibrateHotfilm:

    def __init__(self):
        self.calperiod = np.timedelta64(300, 's')
        self.maxcals = 0
        self.command_line = None
        self.sonics = None
        self.inputs = None
        self.netcdf = 'hotfilm_wind_speed_%Y%m%d_%H%M%S.nc'
        self.images = 'hotfilm_calibrations_%Y%m%d_%H%M%S.png'
        # if true, save calibration plots when generated
        self.plot = False

    def set_command_line(self, argv):
        command_line = " ".join([f"'{arg}'" if ' ' in arg else arg
                                 for arg in argv])
        self.command_line = command_line

    def calibrate_hotfilm(self, filename: str) -> HotfilmWindSpeedDataset:
        maxcals = self.maxcals
        sonics = self.sonics
        calperiod = self.calperiod
        films = HotfilmDataset().open(filename)
        logger.debug("\nhotfilm.timev=%s", films.timev)
        # start with first time in hotfilm dataset rounded to cal period.
        first = films.timev.data[0]
        last = films.timev.data[-1]
        logger.debug("first=%s, type=%s", first, type(first))
        begin = rdatetime(first, calperiod)
        end = rdatetime(last, calperiod)
        end = end + calperiod if end < last else end
        speeds = HotfilmWindSpeedDataset()

        # Compute calibrations
        ncals = 0
        logger.info("begin=%s, end=%s, ncals=%d", begin, end, maxcals)
        while begin < end and (not maxcals or ncals < maxcals):
            cals = []
            for ch in films.dataset.data_vars:
                eb = films.get_variable(ch)
                logger.debug("\neb=%s", eb)
                try:
                    hfc = HotfilmCalibration()
                    hfc.calibrate_winds(sonics, eb, begin, calperiod)
                    speeds.add_wind_speed(hfc, eb)
                    cals.append(hfc)
                    ncals += 1
                except Exception as e:
                    logger.error(f"calibration failed for {eb.name} "
                                 f"at {dt_string(begin)}: {e}")
                if maxcals and ncals >= maxcals:
                    break
            if self.plot:
                self.save_calibration_images(cals)
            begin = begin + calperiod
        films.close()
        return speeds

    def run_calibration(self):
        for filename in self.inputs:
            speeds = self.calibrate_hotfilm(filename)
            ds = speeds.dataset
            add_history_to_dataset(ds, "calibrate_hotfilm", self.command_line)

            if not speeds.dataset.data_vars:
                logger.error("no wind speeds for %s", filename)
                continue
            speeds.save(self.netcdf)

    def run_plots(self):
        speeds = HotfilmWindSpeedDataset()
        for filename in self.inputs:
            speeds.open(filename)
            ctimes = speeds.get_calibration_times()
            spdvars = speeds.get_speed_variables()
            for when in ctimes.data:
                cals = []
                for speed in spdvars:
                    hfc = speeds.get_calibration(when, speed)
                    cals.append(hfc)
                self.save_calibration_images(cals)

    def save_calibration_images(self, cals: list[HotfilmCalibration]):
        """
        Save calibration plots, one file for each calibration time.
        """
        filename = self.images
        ctime = None
        icol = 0
        fig = None
        for hfc in cals + [None]:
            # save previous figure if time has changed or end of list
            if ctime and (not hfc or hfc.begin != ctime):
                when = pd.to_datetime(ctime)
                path = when.strftime(filename)
                logger.info("saving %s", path)
                fig.savefig(path)
                plt.close(fig)
                ctime = None
            if hfc and ctime is None:
                # width:height ratio of 4:1 for square channel plots
                fig = plt.figure(figsize=(20, 5))
                axs = fig.subplots(1, 4, squeeze=False)
                ctime = hfc.begin
                icol = 0
            if hfc:
                hfc.plot(axs[0, icol])
                icol += 1


def main():
    xr.set_options(display_expand_attrs=True, display_expand_data=True)

    calfilm = CalibrateHotfilm()
    calfilm.set_command_line(sys.argv)

    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('inputs', nargs='+',
                        help='With --calibrate, inputs are '
                        'hotfilm voltage NetCDF files '
                        'to calibrate with ISFS sonic data files, '
                        'specified with --sonics.  '
                        'With only --plot, inputs are '
                        'hotfilm wind speed netcdf files.')
    parser.add_argument('--plot', action='store_true',
                        help='Write hotfilm calibration plots.  '
                        'If not also generating the calibrations, then '
                        'specify hotfilm wind speed files as inputs.')
    parser.add_argument('--calibrate', action='store_true',
                        help='Calibrate hotfilm voltages against sonic '
                        'wind speeds.  Inputs are hotfilm voltage files.')
    parser.add_argument('--sonics',
                        help='Directory path to ISFS NetCDF files '
                        'with sonic wind components, possibly with '
                        'a filename pattern with time specifiers to override '
                        'the default filename pattern.')
    parser.add_argument('--images', default=calfilm.images,
                        help='Specify filename pattern for PNG plot files.')
    parser.add_argument('--netcdf', default=calfilm.netcdf,
                        help="Specify filename pattern for hot film "
                        "wind speed NetCDF files.")
    parser.add_argument('--ncals', type=int, default=0,
                        help='Number calibrations to compute or plot, or 0')
    parser.add_argument('--log', help='Log level', default='info')

    args = parser.parse_args()
    level = logging.getLevelNamesMapping()[args.log.upper()]
    logging.basicConfig(level=level)

    if args.calibrate and not args.sonics:
        sys.stderr.write("calibration requires --sonics\n")
        sys.exit(1)
    calfilm.maxcals = args.ncals
    calfilm.inputs = args.inputs
    if args.sonics:
        calfilm.sonics = IsfsDataset(args.sonics)
    calfilm.netcdf = args.netcdf
    calfilm.images = args.images
    calfilm.plot = args.plot

    if args.calibrate:
        calfilm.run_calibration()
    elif args.plot:
        calfilm.run_plots()
    else:
        sys.stderr.write("specify --calibrate, --plot, or both\n")
        sys.exit(1)


if __name__ == "__main__":
    main()
    sys.exit(0)
