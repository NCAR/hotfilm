# Changelog

## [unreleased] - Pending changes

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
[unreleased]: https://github.com/NCAR/hotfilm/compare/v1.0...HEAD
[1.0]: https://github.com/NCAR/hotfilm/releases/tag/v1.0
