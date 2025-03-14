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


logger = logging.getLogger(__name__)


def plot_calibrations(nrows, ncols, cals: list[HotfilmCalibration]):
    # Create panel of calibration plots
    fig, axs = plt.subplots(nrows, ncols, squeeze=False)
    nplots = nrows * ncols

    def subplot(iplot):
        if nrows * ncols == 1:
            return axs
        return axs[iplot // ncols, iplot % ncols]

    dname = cals[0].eb.dims[0]
    begin = cals[0].eb[dname].data[0]
    end = cals[-1].eb[dname].data[-1]
    title = f'Calibrations from {dt_string(begin)} to {dt_string(end)}'
    for iplot, hfc in enumerate(cals[:nplots]):
        hfc.plot(subplot(iplot))
    fig.suptitle(title)
    plt.show()


def save_calibration_images(cals: list[HotfilmCalibration], filename: str):
    """
    Save calibration plots, one file for each calibration time.
    """
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
    ncpattern = 'hotfilm_wind_speed_%Y%m%d_%H%M%S.nc'
    plotpattern = 'hotfilm_calibrations_%Y%m%d_%H%M%S.png'
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('hotfilms',
                        help='NetCDF file with hotfilm voltages')
    parser.add_argument('sonics',
                        help='ISFS NetCDF file with sonic wind components')
    parser.add_argument('--plot', action='store_true',
                        help='Show calibration plots on screen')
    parser.add_argument('--images', const=plotpattern, nargs='?',
                        help='Write calibration plots to PNG files',
                        default=None)
    parser.add_argument('--netcdf', const=ncpattern, nargs='?',
                        help="Write hot film wind speeds to NetCDF",
                        default=None)
    parser.add_argument('--ncals', type=int,
                        help='Number of calibrations to compute, else 0',
                        default=0)
    parser.add_argument('--log', help='Log level', default='info')

    args = parser.parse_args()
    level = logging.getLevelNamesMapping()[args.log.upper()]
    logging.basicConfig(level=level)
    filename = args.hotfilms
    films = HotfilmDataset().open(filename)
    logger.debug("\nhotfilm.timev=%s", films.timev)

    calperiod = np.timedelta64(300, 's')
    # start with first time in hotfilm dataset rounded to cal period.
    first = films.timev.data[0]
    last = films.timev.data[-1]
    logger.debug("first=%s, type=%s", first, type(first))
    begin = rdatetime(first, calperiod)
    end = rdatetime(last, calperiod)
    sonics = IsfsDataset().open(args.sonics)
    cals = []

    speeds = HotfilmWindSpeedDataset()

    # Compute calibrations
    end_sonics = sonics.timev.data[-1]
    ncals = args.ncals
    logger.info("begin=%s, end=%s, ncals=%d", begin, end, ncals)
    while begin < end and (not ncals or len(cals) < ncals):
        if begin > end_sonics:
            logger.warning(f"No sonic wind speed data past "
                           f"{dt_string(end_sonics)}.")
            break
        for ch in films.dataset.data_vars:
            eb = films.get_variable(ch)
            logger.debug("\neb=%s", eb)
            try:
                hfc = HotfilmCalibration()
                hfc.calibrate_winds(sonics, eb, begin, calperiod)
                cals.append(hfc)
                speeds.add_wind_speed(hfc, eb)
            except Exception as e:
                logger.error(f"calibration failed for {eb.name} "
                             f"at {dt_string(begin)}: {e}")
                raise
            if ncals and len(cals) >= ncals:
                break
        begin = begin + calperiod

    if args.netcdf:
        speeds.save(args.netcdf)
    elif args.images:
        save_calibration_images(cals, args.images)
    elif args.plot:
        plot_calibrations(2, 4, cals)
    else:
        logger.error("Select output with --plot, --images, or --netcdf.")
    films.close()


if __name__ == "__main__":
    main()
    sys.exit(0)
