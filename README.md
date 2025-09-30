# Hot Film Data Acquistion, Processing, and Calibration

This repository contains code to sample hotfilm AD channels and record the
channels to disk in [NIDAS](https://github.com/NCAR/nidas) dat archive format.
It also contains Python code for processing the raw voltage data, calibrating
against sonic wind speed, and writing the data to NetCDF files.

Specific information on the hot films deployed for the M2HATS project can be
found on the [hot films](https://wiki.ucar.edu/display/M2HATSISFS/hot+films)
wiki page on the [M2HATS Logbook
Wiki](https://wiki.ucar.edu/display/M2HATSISFS/).

## Documentation:

- [Changelog](Changelog.md)
- [Processing](docs/Processing.md): Process raw hot film voltage data and
  write to text or netcdf.
- [Calibration](docs/Calibration.md): Calibrate hot film voltages to derive
  wind speed.
- [Installation](Install.md): Install the hardware and software to run the
  data acquisition.
- [Data Acquisition](docs/Data-Acquisition.md): Run the data acquisition
  program to record hot film voltage data.  This also contains important
  information about the variables and diagnostics stored in the raw data.
