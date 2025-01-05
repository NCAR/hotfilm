
from pytest import approx
import logging
import numpy as np
from read_winds import ISFSDataset, rdatetime


logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.DEBUG)


def test_rdatetime():
    ns = np.timedelta64(1, 'ns')
    us = np.timedelta64(1, 'us')
    ms = np.timedelta64(1, 'ms')
    s = np.timedelta64(1, 's')
    testdata = [
        ('2023-08-04T16:00:00.016666667', ms, '2023-08-04T16:00:00.017'),
        ('2023-08-04T16:00:00.016666667', us, '2023-08-04T16:00:00.016667'),
        ('2023-08-04T16:00:00.016666667', ns, '2023-08-04T16:00:00.016666667'),
        ('2023-08-04T16:00:00.000000500', us, '2023-08-04T16:00:00.000001000'),
        ('2023-08-04T16:00:00.000000500', ms, '2023-08-04T16:00:00.000'),
        ('2023-08-04T16:00:00.501', s, '2023-08-04T16:00:01'),
        ('2023-08-04T16:00:00.499', s, '2023-08-04T16:00:00'),
    ]

    for when, period, expected in testdata:
        when = np.datetime64(when)
        expected = np.datetime64(expected)
        assert rdatetime(when, period) == expected


def test_read_winds():
    ids = ISFSDataset().open("test_data/u_2m_t0_20230804_160000.nc")
    ds = ids.dataset
    u = ids.get_variable("u_2m_t0")
    first = ds.time[0]
    last = ds.time[-1]
    u = ids.reshape_variable(u)
    assert len(u.values) == 3600 * 60
    assert u.values[0] == approx(0.06215948)
    assert u.values[-1] == approx(-0.145788)
    dtime = u.dims[0]
    # the first half-second timestamp at 500 ms does not match exactly the
    # 30th timestamp at 16666666 ns, but it should round to exactly 500 ms.
    us = np.timedelta64(1, 'us')
    assert rdatetime(u.coords[dtime][30].values, us) == first
    xlast = last.values + 29 * np.timedelta64(16666666, 'ns')
    assert rdatetime(u.coords[dtime][-1].values, us) == rdatetime(xlast, us)
    ds.close()
