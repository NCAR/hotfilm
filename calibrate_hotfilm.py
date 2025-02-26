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

    begin = cals[0].eb.time.data[0]
    end = cals[-1].eb.time.data[-1]
    title = f'Calibrations from {dt_string(begin)} to {dt_string(end)}'
    for iplot, hfc in enumerate(cals[:nplots]):
        hfc.plot(subplot(iplot))
    fig.suptitle(title)
    plt.show()


def main():
    logging.basicConfig(level=logging.DEBUG)
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
    sonics = IsfsDataset().open(sys.argv[2])
    cals = []

    speeds = HotfilmWindSpeedDataset()

    # Compute calibrations
    while begin < end:
        next_time = begin + calperiod
        # select voltage and wind speeds
        # use open-ended slice for next_time
        end_slice = next_time - np.timedelta64(1, 'ns')
        for ch in films.dataset.data_vars:
            eb = films.get_variable(ch)
            logger.debug("\neb=%s", eb)
            try:
                hfc = HotfilmCalibration()
                hfc.calibrate_winds(sonics, eb, begin, end_slice)
                cals.append(hfc)
            except Exception as e:
                logger.error(f"calibration failed: {e}")
                continue
            hfspd = hfc.convert_to_wind_speed(eb)
            logger.debug("hfspd:\n%s", hfspd)
            speeds.add_wind_speed(hfspd)
            speeds.add_calibration(hfc.as_dataset())
        begin = next_time

    # plot_calibrations(2, 4, cals)
    logger.debug("%d calibrations done.", len(cals))
    speeds.save("hotfilm_wind_speed_%Y%m%d_%H%M%S.nc")
    films.close()


if __name__ == "__main__":
    main()
    sys.exit(0)
