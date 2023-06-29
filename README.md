# Hot Film Sampling with LabJack

This repository contains code to use a [Labjack T7](https://labjack.com/t7) to
sample at least 4 hotfilm AD channels at up to 2 KHz and record the channels
to disk in NIDAS dat archive format.

Links:

- [Installation](Install.md)
- [Changelog](Changelog.md)
- [LabJack U6](LabJackU6.md)

## Running

The `hotfilm` program tries to set a realtime FIFO scheduling policy with
priority 50.  It can be started as root so it has permissions to set the
scheduling parameters, and then it can switch to a different user:

```plain
daq@dsm214:~/hotfilm $ sudo -s  ./hotfilm -u daq --log info --xml hotfilm.xml 
[sudo] password for daq: 
2023-06-29,21:32:57|INFO|thread policy=1, priority=50
2023-06-29,21:32:57|NOTICE|parsing: hotfilm.xml
...
```

## Data output

Like for a NIDAS `dsm` process, the `hotfilm` process is configured with an
XML file which can specify multiple sample outputs, usually a file archive and
a sample server socket.

Unlike the sensors in a normal `dsm` process, the samples recorded by
`hotfilm` are not raw character streams which have to be processed to generate
samples with numbers corresponding to variables.  Instead, the output samples
already contain floating point numbers (*float* type) broken out into different
samples for each channel and different samples for diagnostics.  The sample IDs
can be used to differentiate and filter them easily.

In [hotfilm.xml](hotfilm.xml), the sensor class in the definition is just a
placeholder.  That sensor class never actually reads any data into samples or
processes any raw samples.

At the moment, the IDs in the `hotfilm` output samples are hardcoded to match
the ones in the `hotfilm.xml` file.  When recording samples, the `hotfilm.xml`
file is only used to setup the sample outputs.  When reading the output
samples, the XML is used to asociate tag information with the samples
according to the sample IDs.

Current versions of NIDAS utilities like `data_dump` and `data_stats` expect
file archives to have only raw samples containing character data.  They will
filter samples at best or possibly crash if `-p` is used with the files.  This
is being fixed on the `buster` branch and will eventually be fixed in a NIDAS
release.

## Diagnostics

This is an example of using `data_dump` to show all the 1-second statistics
and diagnostics without the full 2000-point 1-second time series:

```plain
daq@dsm214:~/hotfilm $ data_dump -i -1,501 -i -1,510-513 hotfilm_20230629_203645.dat 
2023-06-29,21:32:08|INFO|opening: hotfilm_20230629_203645.dat
2023-06-29,21:32:08|NOTICE|parsing: hotfilm.xml
|--- date time --------|  deltaT   id          len data...
2023 06 29 20:36:45.7860       0 200, 501      20          1        428          0         48        506 
2023 06 29 20:36:45.7860       0 200, 510      12 0.00052848 -0.0036095  0.0049213 
2023 06 29 20:36:45.7860       0 200, 511      12  0.0046688 0.00081343   0.052004 
2023 06 29 20:36:45.7860       0 200, 512      12 -0.0075408  -0.036148   0.014717 
2023 06 29 20:36:45.7860       0 200, 513      12 0.00053158 -0.0074004  0.0087132 
2023 06 29 20:36:46.7860       1 200, 501      20          2        428          0         96        510 
Exception: EOFException: hotfilm_20230629_203645.dat: open: EOF
```

Sample 501 has 5 variables:

- PPS count: latest counter value for the PPS DIO channel.
- PPS index (aka step): index of the counter change in the last read buffer
  (out of 1000 samples, half the scan rate)
- Device scan backlog: scans left in the device buffer after the last read;
  should be near zero and not increasing.
- Host scan backlog: scans still in the host-side buffer; should be near zero
  and not increasing.
- Time of last read in ms: this should be close to 500 ms.  The stream is
  configured to read half a second of samples at a time.  So the time spent in
  the read call should be mostly waiting for a half-second of scans to fill
  up, or about 500 ms.  If it gets small then the host sample writing has
  fallen behind and is catching up.  If it gets large then the reads from the
  device are being delayed, such as by network congestion or delays in the LJM
  library itself.

Samples 510-513 are the avg/min/max for channels 0-3 over the full second of
data in the corresponding samples 520-523.

Showing stats on any of the samples can indicate if there are any
synchronization issues.  The rate and the min/max time between samples should
be 1.0 on any continuously running sample stream:

```plain
daq@dsm214:~/hotfilm $ data_stats -i 200,501 hotfilm_20230629_213259.dat  hotfilm_20230629_220000.dat 
2023-06-29,22:38:38|NOTICE|parsing: hotfilm.xml
Exception: EOFException: hotfilm_20230629_220000.dat: open: EOF
sensor  dsm sampid    nsamps |------- start -------|  |------ end -----|    rate minMaxDT(sec) minMaxLen
        200    501      3939 2023 06 29 21:32:59.101  06 29 22:38:37.104    1.00  1.000  1.001   20   20
```

Logging can also be helpful.  Turn on debugging log messages with `--log
debug`.  The `--diag` command-line argument enables extra LJM calls to report
on the TCP buffer status and check for skipped scans.  However, for normal
operations, that probably adds more overhead than it's worth.

## Todo

The diagnostic samples can be viewed in real-time using the tips above, but
they are not yet available through a DSM dashboard.  The diagnostics could be
relayed as UDP packets to another DSM instance, which then includes those
variables in the dashboard.  Or `hotfilm` could run on its own DSM with its
own dashboard, and the `json_data_stats` service could be configured to dump
only the diagnostic samples.

## Implementation Notes

The `hotfilm` program runs similarly to the NIDAS `dsm` process.  However,
rather than `DSMEngine` controlling the sensor opening, polling, and reading,
the `hotfilm` program calls the LabJackM library in sequence to open the
device and read the stream, blocking where needed.  This simplifies the logic
of the program and the use of the LabJackM library.  The program is also built
outside of the NIDAS source tree, so it can be built easily on the DSM3 Pi
against buster branch, and so the LabJackM library does not need to be linked
into NIDAS.

The LabJackM library does provide a callback API using
[SetStreamCallback](https://labjack.com/pages/support?doc=/software-driver/ljm-users-guide/setstreamcallback/),
so the callback can be used to notify when the stream buffer is full and ready
to read with
[LJM_eStreamRead](https://labjack.com/pages/support?doc=/software-driver/ljm-users-guide/estreamread/).
That could allow the stream reads to be integrated with the NIDAS
SensorHandler, if the LabJack sensor provided something like a file descriptor
on a pipe to which the callback could write to indicate data are ready to be
read.

### Stream Mode

[This page of the T-series
datasheet](https://labjack.com/pages/support?doc=/datasheets/t-series-datasheet/30-communication-t-series-datasheet/)
implies that a 2 KHz scan rate should use Stream mode rather than
command-response mode, so that is what `hotfilm` uses.

### PPS Counter

The DIO0 channel is configured as a counter to detect PPS pulses.  Given
register settings below:

```plain
DIO0_EF_ENABLE=0
DIO0_EF_INDEX=8
DIO0_EF_ENABLE=1
```

then `DIO0_EF_READ_A` will be the current counter, and that channel can be
streamed also.

- [DIO extended features page](https://labjack.com/pages/support?doc=%2Fdatasheets%2Ft-series-datasheet%2F132-dio-extended-features-t-series-datasheet%2F)
- [Interrupt Counter](https://labjack.com/pages/support?doc=/datasheets/t-series-datasheet/1329-interrupt-counter-t-series-datasheet/)

On the DSM3, PPS is on GPIO26.  That can be wired to the LabJack FIO0 input
using the handy stackable [Pi-EzConnect breakout
board](https://www.adafruit.com/product/2711).  One of the GND screw terminals
on the breakout has to be wired to the GND input next to the FIO0 terminal.

The LabJack T7 also supports external triggers, so in theory the PPS could be
used to trigger the start of a scan, and that might save some overhead from
streaming the pulse counter at the same rate as the AIN channels.  However,
that option was rejected so that sampling can happen even in the absence of
the PPS.  If the counter synchronization proves reliable, then the full
counter stream could be left out of the recorded data to at least avoid that
overhead.

### Differential

All the analog inputs are configured as differentials, so each channel
requires a pair of analog input terminals.  Channel 0 is AIN0+ and AIN1-,
channel 1 is AIN2+ and AIN3-.  In the code, the even inputs are named in the
scan list: AIN0, AIN2, AIN4, AIN6.

### Resolution

The scan resolution is set to 0, the default, which should result in the
highest possible resolution index of 8 for the 2 KHz scan rate.

LJM provides the option to read 16-bit data instead of converting to float on
the host side.  However, there does not seem to be any disadvantage to
recording the data already scaled to Volts.  One downside is that 32-bit
floats take twice as much space, but that is unlikely to be a problem.

