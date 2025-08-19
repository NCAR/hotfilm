
from pathlib import Path
import tempfile
import logging
import pandas as pd
import xarray as xr
import numpy as np

logger = logging.getLogger(__name__)


class OutputPath:

    def __init__(self):
        self.when = None
        self.path = None
        self.tfile = None
        self.filespec = None

    def start(self, filespec: str, data: xr.Dataset):
        when = pd.to_datetime(data.time.data[0])
        path = Path(when.strftime(filespec))
        tfile = tempfile.NamedTemporaryFile(dir=str(path.parent),
                                            prefix=str(path.name)+'.',
                                            delete=False)
        logger.debug("starting file: %s", tfile.name)
        self.when = when
        self.path = path
        self.tfile = tfile
        self.filespec = filespec
        return tfile

    def remove(self):
        if not self.tfile:
            return
        logger.debug("removing temp file: %s", self.tfile.name)
        self.tfile.close()
        Path(self.tfile.name).unlink(missing_ok=True)
        self.tfile = None

    def finish(self, period: np.timedelta64 = None,
               when: np.datetime64 = None) -> Path:
        """
        Rename the temporary file to the final filename.  Insert the period
        length in minutes if set, and @p when is not None, use it to generate
        the filename.
        """
        path = self.path
        if when:
            path = Path(pd.to_datetime(when).strftime(self.filespec))
        minutes = None
        if period is not None:
            minutes = np.timedelta64(period, 'm').astype(int)
        if minutes is None:
            fpath = path.name
        else:
            fpath = path.stem + ("_%03d" % (minutes)) + path.suffix
        fpath = Path(self.tfile.name).parent / fpath
        logger.info("file finished, renaming: %s", fpath)
        fpath = Path(self.tfile.name).replace(fpath)
        # the files should not need to be writable
        fpath.chmod(0o444)
        self.tfile = None
        self.path = None
        self.when = None
        return fpath
