#! /bin/env python
"""
Given a hotfilm voltage dataset and an ISFS dataset with sonic wind component
variables, calculate calibration coefficients for the hotfilm channel voltages
and compute wind speed from them.
"""
import sys
import logging
import matplotlib.pyplot as plt
import numpy as np

from hotfilm.isfs_dataset import IsfsDataset, rdatetime
from hotfilm.hotfilm_dataset import HotfilmDataset
from hotfilm.hotfilm_dataset import HotfilmCalibration
from hotfilm.hotfilm_dataset import dt_string


logger = logging.getLogger(__name__)


def main():
    logger.setLevel(logging.DEBUG)
    filename = sys.argv[1]
    films = HotfilmDataset().open(filename)
    logger.debug("\nhotfilm.timev=%s", films.timev)

    calperiod = np.timedelta64(300, 's')
    # start with first time in hotfilm dataset rounded to cal period.
    first = films.timev.data[0]
    last = films.timev.data[-1]
    logger.debug("first=%s, type=%s", first, type(first))
    begin = rdatetime(first, calperiod)
    end = rdatetime(last, calperiod)

    eb = films.get_variable('ch3')
    logger.debug("\neb=%s", eb)
    sonics = IsfsDataset().open(sys.argv[2])
    u, _, w = sonics.get_wind_variables(eb)
    spd = sonics.get_speed(u, w)
    logger.debug("\nspd=%s", spd)
    cals = []

    # Compute calibrations
    while begin < end:
        next_time = begin + calperiod
        # select voltage and wind speeds
        # use open-ended slice for next_time
        end_slice = next_time - np.timedelta64(1, 'ns')
        try:
            hfc = HotfilmCalibration().calibrate(spd, eb, begin, end_slice)
            cals.append(hfc)
        except Exception as e:
            logger.error(f"calibration failed: {e}")
        begin = next_time

    # Create panel of calibration plots
    nrows, ncols = 2, 3
    fig, axs = plt.subplots(nrows, ncols)
    nplots = nrows * ncols

    def subplot(iplot):
        if nrows * ncols == 1:
            return axs
        return axs[iplot // ncols, iplot % ncols]

    title = f'Calibrations from {dt_string(begin)} to {dt_string(end)}'
    for iplot, hfc in enumerate(cals[:nplots]):
        hfc.plot(subplot(iplot))
    fig.suptitle(title, fontsize=16)
    plt.show()

    films.close()


if __name__ == "__main__":
    main()
    sys.exit(0)
