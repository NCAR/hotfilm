
import numpy as np

from hotfilm_dataset import HotfilmDataset


def test_hotfilm_dataset():
    hfd = HotfilmDataset().open("baseline/channel2_20230804_180000_005.nc")
    ds = hfd.dataset
    xlen = 5*60*2000
    assert len(ds.time) == xlen
    xfirst = np.datetime64('2023-08-04T18:00:00.804500')
    assert ds.time[0].values == xfirst
    xlast = xfirst + (xlen - 1) * np.timedelta64(500, 'us')
    assert ds.time[-1].values == xlast
    hfd.dataset.close()
