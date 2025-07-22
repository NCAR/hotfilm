"""
Test HotfilmWindSpeedDataset.
"""

import logging
from pathlib import Path

from hotfilm.calibrate_hotfilm import CalibrateHotfilm

logger = logging.getLogger(__name__)


_this_dir = Path(__file__).parent.absolute()


def test_hotfilm_wind_speed_dataset():

    chf = CalibrateHotfilm()
    chf.set_process_window("2023-08-04T18:30:00", "2023-08-04T18:35:00")

    xpng = _this_dir / "test_out" / "hotfilm_calibrations_20230804_183000.png"
    xpng.unlink(missing_ok=True)

    xnc = _this_dir / "test_out" / "hotfilm_wind_speed_20230804_183000.nc"
    xnc.unlink(missing_ok=True)

    inputs = Path("test_data") / "hotfilm_20230804_182917_6.nc"
    netcdf = Path("test_out") / chf.netcdf
    images = Path("test_out") / chf.images
    sonics = Path("test_data") / "isfs_m2hats_qc_hr_inst_uvw_20230804_183000.nc"
    chf.plot = True
    chf.set_command_line(f"calibrate_hotfilm.py --plot "
                         f"--images {images} --netcdf {netcdf} "
                         f"--sonics {sonics} {inputs}".split())
    logger.debug(f"{chf.command_line}")
    chf.inputs = [str(_this_dir / inputs)]
    chf.netcdf = str(_this_dir / netcdf)
    chf.images = str(_this_dir / images)
    sonics = str(_this_dir / sonics)

    logger.debug("hotfilm wind speed dataset: %s", sonics)
    chf.run_calibration(sonics)

    assert xpng.exists(), f"Expected plot image {xpng} does not exist."
    assert xnc.exists(), f"Expected netcdf file {xnc} does not exist."
