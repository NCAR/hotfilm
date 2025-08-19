# Changelog

## [unreleased] - Pending changes

Added the `--interval` argument to `dump_hotfilm.py` to write the netcdf files
at fixed intervals, by default one hour.  The `--min` and `--max` arguments
now default to 0, so unless they are specified, all data are written and file
start times are determined only by `--interval`.

The `dump_hotfilm.py` script now writes out the hotfilm voltage timestamps
exactly as they were recorded in the raw NIDAS data files, instead of
adjusting the timestamps so contiguous samples had a regularly spaced time
interval matching the nominal sample frequency.  This means that even
continuously sampled data will have occasional jumps in timestamps greater
than the expected sample interval, due to the ADC clock drifting relative to
GPS time.  The previous behavior can be selected with `--keep-contiguous`.

Add $R^2$ to the calibration quality of fit diagnostics.  See the notes in
[Calibration.md](docs/Calibration.md).

## [1.3] - 2025-03-22

The `calibrate_hotfilm.py` script has been created.  It reads hotfilm voltage
netcdf files and ISFS sonic netcdf files to calculate calibration fits, then
uses those calibrations to calculate wind speed from the hotfilm voltages and
write the results to netcdf.  The netcdf files contain each calibration,
including the coefficients, RMS, the mean u, v, and w from the sonics, and the
mean voltage and wind speed used to compute the least squares fit to the
calibration equation.  Calibration plots can be saved either while generating
the calibrations or afterwards directly from the netcdf files.

Raw data dumps can now write text csv or netcdf, for either 2K or 4K data.
There is an algorithm which tries to detect contiguous blocks of data, then
adjusts the timestamps of subsequent blocks to match the sampling rate, to
account for clock drift between the ADC clock and GPS.  New output files are
started when there is missing data (meaning GPS sync was lost or a buffer
overrun occurred) or clock drift gets too large.

Use the `--precision` flag to `data_dump` to print data values with full
precision.

## [1.2] - 2023-09-07

In support of 4K sampling, constrain the scan rate option to divide evenly
into 1e6 microseconds and be divisible by the allowed read rates.  The read
rate can be 1, 2, or 4 reads per second, in case modifiying the size of each
read (which also depends on the scan rate and number of channels) improves the
overall network throughput and avoids dropped scans from device buffer
overflows.

## [1.1] - 2023-08-09

Set STREAM_BUFFER_SIZE_BYTES in the LabJack to the maximum value of 32768,
about 2 seconds of scans, in an attempt to overcome probable networking delays
causing a buffer overrun in the device and resulting in dummy values (-9999)
in the data returned by the LJM library.

Detect -9999 in `pps_count` changes and log them as notices, so that lost
scans can be seen in the journal and not only by looking at the data.

Log version information on startup.

## [1.0] - M2HATS first release

This is the version of the acquisition code (hotfilm.cc) that has run since
M2HATS started.  A few changes related to installation and monitoring were
made soon after the project started, but this version contains those files as
they have been since then for the bulk of the project.

There are scons targets which install the hotfilm executable into the NIDAS
bin directory and also set capability flags on the file to allow realtime
scheduling priority without running as root.

Most of the LabJack stream configuration registers can be modified through
command-line arguments, including rate, resolution index, number of channels,
range, settling time, and whether to enable the PPS counter.

Viewing the data with tag information, for either the real-time network stream
or data files, requires changes being committed to the NIDAS buster branch.

Uses the LabJackM streaming mode and API to accumulate scans at 2 KHz into
1-second NIDAS samples and write them to the sample outputs configured in the
XML configuration file.  Sets realtime FIFO scheduling priority to minimize
latency.  Timestamp synchronization seems to work reliably, but might need
improvement.

<!-- Versions -->
[unreleased]: https://github.com/NCAR/hotfilm/compare/v1.3...HEAD
[1.3]: https://github.com/NCAR/hotfilm/releases/tag/v1.3
[1.2]: https://github.com/NCAR/hotfilm/releases/tag/v1.2
[1.1]: https://github.com/NCAR/hotfilm/releases/tag/v1.1
[1.0]: https://github.com/NCAR/hotfilm/releases/tag/v1.0
