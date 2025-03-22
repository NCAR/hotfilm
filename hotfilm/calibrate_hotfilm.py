
import logging

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd

from hotfilm.isfs_dataset import IsfsDataset
from hotfilm.hotfilm_dataset import HotfilmDataset
from hotfilm.hotfilm_wind_speed_dataset import HotfilmWindSpeedDataset
from hotfilm.hotfilm_calibration import HotfilmCalibration
from .utils import dt_string
from .utils import add_history_to_dataset
from .utils import rdatetime


logger = logging.getLogger(__name__)


class CalibrateHotfilm:
    """
    Manage the process of calibrating multiple hotfilm data files against
    sonic datasets and also plotting the calibration results.
    """

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

    def run_calibration(self, sonics_spec: str):
        self.sonics = IsfsDataset(sonics_spec)

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
