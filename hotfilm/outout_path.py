
from pathlib import Path
import tempfile
import logging
import pandas as pd
import xarray as xr

logger = logging.getLogger(__name__)


class OutputPath:

    def __init__(self):
        self.when = None
        self.path = None
        self.tfile = None

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
        return tfile

    def remove(self):
        if not self.tfile:
            return
        logger.debug("removing temp file: %s", self.tfile.name)
        self.tfile.close()
        Path(self.tfile.name).unlink(missing_ok=True)
        self.tfile = None

    def finish(self, minutes: int = None) -> Path:
        path = self.path
        if minutes is None:
            fpath = path
        else:
            fpath = path.stem + ("_%03d" % (minutes)) + path.suffix
        fpath = Path(self.tfile.name).parent / fpath
        logger.debug("file finished with mins=%s, renaming: %s",
                     minutes, fpath)
        fpath = Path(self.tfile.name).rename(fpath)
        # the files should not need to be writable
        fpath.chmod(0o444)
        self.tfile = None
        self.path = None
        self.when = None
        return fpath
