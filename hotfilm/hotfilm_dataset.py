"""
Class to read and write hotfilm netcdf data.
"""
import logging
import xarray as xr


logger = logging.getLogger(__name__)


class HotfilmDataset:

    HEIGHTS = {'ch0': '0.5m', 'ch1': '1m', 'ch2': '2m', 'ch3': '4m'}

    def __init__(self):
        self.dataset = None
        self.timev = None
        self.timed = None

    def open(self, filename):
        logger.info(f"opening hotfilm dataset: {filename}")
        self.dataset = xr.open_dataset(filename)
        self.timev = self.dataset['time']
        self.timed = self.timev.dims[0]
        logging.debug(f"Opened hotfilm dataset: {filename}, %s...%s",
                      self.timev[0], self.timev[-1])
        return self

    def get_variable(self, name: str) -> xr.DataArray:
        eb = self.dataset[name]
        # this should have been in the dataset, so hardcode it until it is
        if 'long_name' not in eb.attrs:
            eb.attrs['long_name'] = f'{eb.name} bridge voltage'
        if 'site' not in eb.attrs:
            eb.attrs['site'] = 't0'
        if 'height' not in eb.attrs:
            eb.attrs['height'] = self.HEIGHTS[eb.name]
        return eb

    def close(self):
        self.dataset.close()
