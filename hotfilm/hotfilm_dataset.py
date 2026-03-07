"""
Class to read and write hotfilm netcdf data.
"""
import logging
import xarray as xr
import numpy as np


logger = logging.getLogger(__name__)


class HotfilmDataset:
    """
    Class to read and write hotfilm netcdf data.  Create the dataset with one
    or more filenames or a directory name, then use it to load hotfilm voltage
    datasets for specific time periods.  Other methods on the dataset provide
    convenient access to common metadata or manipulations.
    """

    HEIGHTS = {'ch0': '0.5m', 'ch1': '1m', 'ch2': '2m', 'ch3': '4m'}

    def __init__(self):
        self.files = []

    def open(self, filename):
        self.add([filename])
        return self

    def add(self, filenames):
        for path in filenames:
            logger.debug(f"opening hotfilm dataset: {path}")
            self.files.append(xr.open_dataset(path, engine='netcdf4'))

    def begin(self) -> np.datetime64 | None:
        "Return the begin time of the dataset, or None if no files were added."
        return self.files[0]['time'][0].data if self.files else None

    def end(self) -> np.datetime64 | None:
        "Return the end time of the dataset, or None if no files were added."
        return self.files[-1]['time'][-1].data if self.files else None

    def load(self, begin: np.datetime64 | None = None,
             end: np.datetime64 | None = None) -> xr.Dataset | None:
        """
        Load the dataset for the given time window.  If begin or end are None,
        they will be set to the begin or end of the dataset, respectively.  If
        the dataset is empty, return None.
        """
        if not self.files:
            return None
        if begin is None:
            begin = self.begin()
        if end is None:
            end = self.end()
        logger.info(f"loading hotfilm dataset: {begin} to {end}")

        ds = self.fix_variables(self.files[0])
        return ds

        # extract and merge datasets that overlap with the requested time
        # window.  i'm not sure if the file dataset time coordinates should be
        # converted to np.datetime64 first, or if the time window coordinates
        # should be converted to the file dataset time units.
        merge = None
        for ds in self.files:
            if begin >= ds['time'][0] and begin <= ds['time'][-1]:
                 merge = ds.sel(time=slice(begin, end))

        self.dataset = xr.open_dataset(filename, engine='netcdf4')
        self.timev = self.dataset['time']
        self.timed = self.timev.dims[0]
        logging.debug(f"Opened hotfilm dataset: {filename}, %s...%s",
                      self.timev[0], self.timev[-1])
        return self

    def fix_variables(self, ds: xr.Dataset) -> xr.Dataset:
        """
        Ensure variables attributes are in the dataset.
        """
        for eb in [v for v in ds.data_vars.values()
                  if isinstance(v.name, str) and v.name.startswith('ch')]:
            if 'long_name' not in eb.attrs:
                eb.attrs['long_name'] = f'{eb.name} bridge voltage'
            if 'site' not in eb.attrs:
                eb.attrs['site'] = 't0'
            if 'height' not in eb.attrs:
                eb.attrs['height'] = self.HEIGHTS[eb.name]
        return ds

    def close(self):
        for ds in self.files:
            ds.close()


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description="Test hotfilm dataset")
    parser.add_argument('filename', nargs='+', dest='filenames',
                        help="hotfilm netcdf file")
    args = parser.parse_args()

    hfd = HotfilmDataset()
    hfd.add(args.filenames)
