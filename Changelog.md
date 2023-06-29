# Changelog

## [Unreleased]

- /

## [0.0.1] - 2023-06-29

- initial release

  Uses the LabJackM streaming mode and API to accumulate scans at 2 KHz into
  1-second NIDAS samples and write them to the sample outputs configured in
  the XML configuration file.  Sets realtime FIFO scheduling priority to
  minimize latency.  Timestamp synchronization seems to work reliably, but
  might need improvement.

  Viewing the data with tag information, for either the real-time network
  stream or data files, requires changes being committed to the NIDAS buster
  branch.

<!-- Versions -->
[unreleased]: https://github.com/NCAR/hotfilm/compare/v0.0.1...HEAD
[0.0.1]: https://github.com/NCAR/hotfilm/releases/tag/v0.0.1
