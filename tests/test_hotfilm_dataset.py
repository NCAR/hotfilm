
from pathlib import Path
import numpy as np

from hotfilm_dataset import HotfilmDataset


_this_dir = Path(__file__).parent
_baseline_dir = _this_dir / "baseline"


def test_hotfilm_dataset():
    hfd = HotfilmDataset().open(_baseline_dir / "channel2_20230804_180000_005.nc")
    ds = hfd.dataset
    xlen = 5*60*2000
    assert len(ds.time) == xlen
    xfirst = np.datetime64('2023-08-04T18:00:00.804500')
    assert ds.time[0].values == xfirst
    xlast = xfirst + (xlen - 1) * np.timedelta64(500, 'us')
    assert ds.time[-1].values == xlast
    hfd.dataset.close()
