#! /bin/env python
"""
Given a hotfilm voltage dataset and an ISFS dataset with sonic wind component
variables, calibrate the hotfilm voltages against sonic wind speed, then write
the computed wind speeds to netcdf, or plot the calibrations.
"""
import sys
import logging
import argparse

import xarray as xr
from hotfilm.calibrate_hotfilm import CalibrateHotfilm

logger = logging.getLogger(__name__)


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
    calfilm.netcdf = args.netcdf
    calfilm.images = args.images
    calfilm.plot = args.plot

    if args.calibrate:
        calfilm.run_calibration(args.sonics)
    elif args.plot:
        calfilm.run_plots()
    else:
        sys.stderr.write("specify --calibrate, --plot, or both\n")
        sys.exit(1)


if __name__ == "__main__":
    main()
    sys.exit(0)
