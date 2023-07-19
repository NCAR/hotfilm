# Changelog


## [0.0.1] - Pending initial release

  There are scons targets which install the hotfilm executable into the NIDAS
  bin directory and also set capability flags on the file to allow realtime
  scheduling priority without running as root.

  Most of the LabJack stream configuration registers can be modified through
  command-line arguments, including rate, resolution index, number of
  channels, range, settling time, and whether to enable the PPS counter.

  Viewing the data with tag information, for either the real-time network
  stream or data files, requires changes being committed to the NIDAS buster
  branch.

  Uses the LabJackM streaming mode and API to accumulate scans at 2 KHz into
  1-second NIDAS samples and write them to the sample outputs configured in
  the XML configuration file.  Sets realtime FIFO scheduling priority to
  minimize latency.  Timestamp synchronization seems to work reliably, but
  might need improvement.

<!-- Versions -->
[unreleased]: https://github.com/NCAR/hotfilm/compare/v0.0.1...HEAD
[0.0.1]: https://github.com/NCAR/hotfilm/releases/tag/v0.0.1
