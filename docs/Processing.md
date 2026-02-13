# Hot film data processing

These are notes about reading the raw data files and converting them to other
output formats, possibly with calibrations to wind speed.

## What is a Scan

Scan is used in two ways in hotfilm data processing:

- In the LabJack AD acquisition, a scan is one scan of all the channels, and
  scans happen at the scan rate, also sometimes called the sample rate, such
  as 2 KHz or 4 KHz.  A full second of scans for a channel is collected into
  one NIDAS raw sample, with one variable containing all of the scans, such as
  2000 or 4000 values.

- In the `dump_hotfilm.py` script, each channel is read from individual
  samples in separate lines of `data_dump` output.  So multiple lines of data
  must be assembled into a complete 1-second scan of data, where the 1-second
  scan contains all the channels and other housekeeping variables.

## Exporting hotfilm data as text

The script [dump_hotfilm.py](dump_hotfilm.py) can translate NIDAS archive
files into a column text format using `data_dump`.  Run `dump_hotfilm.py -h`
to see usage.

This is the command used to export data for M2HATS on `ustar`.  The text files
have two columns, first column is floating point seconds since the epoch, and
the second column is channel 1, ie, the hotfilm at the 1m sonic.

```plain
dump_hotfilm.py --log info --channel 1 --timeformat %s.%f --text text_%Y%m%d_%H%M%S.epoch.txt /data/isfs/projects/M2HATS/raw_data/
```

The script creates output files of uninterrupted, contiguous scans, by default
at least 30 minutes and no more than 4 hours.  The min and max limits can be
adjusted with command-line arguments.

The text files can be compressed afterwards.

## Exporting to NetCDF

The [dump_hotfilm.py](dump_hotfilm.py) script can also write netcdf using the
`--netcdf` option.  There are several options available through command-line
arguments, but the defaults are set according to the production preferences:

- hourly data files named with the exact hour
- hotfilm times are the best known absolute times, they are not shifted to the
  requested sampling frequency

Use `-h` to see the full usage.

## Data issues

These are known issues related to the data processing, and how problems are
corrected in the output when possible.

### Time shifts caused by system delays

There are cases in the raw data where the hotfilm sample times jump forwards
and backwards even though there were no interruptions to the data and no
buffer underflows or overflows:

```
$ data_dump --nolen --precision 8 --iso -i 200,501 hotfilm_20230920_010000.dat
2026-02-06,14:44:09|INFO|opening: hotfilm_20230920_010000.dat
|--- date time --------|  deltaT data...
2023-09-20T01:00:00.8450    0    38832   620   1     88   514   521525
2023-09-20T01:00:03.8450    3    38833   620   1   8171     0   1058911
2023-09-20T01:00:04.8450    1    38834   620   1   4171     0   61663
2023-09-20T01:00:03.8450   -1    38835   620   1    171     0   1067025
2023-09-20T01:00:04.8450    1    38836   620   0     62   507   517327
```

This was likely caused when system scheduling delays or output blocking caused
the hotfilm acquisition process to get behind reading the data from the
LabJack host buffer, so hotfilm software assigned timestamps to the scans
relative to the wrong system time.  Note the device scan buffer is almost
empty (0 and 1), while the host buffer is accumulating thousands of scans.
See [Data-Acquisition.md](Data-Acquisition.md) for details. The scans are
contiguous because the PPS counter is consecutive, and there are no fill
values (-9999) in the data.

Raw hotfilm sample times were sometimes shifted by whole seconds when the
wrong system time was used to derive the PPS time, and this shift could
persist for hours.  These jump corrections are tracked in one notice per
netcdf file.  Since the `pps_count` and `pps_step` are correct and there are
no missing values, the times are corrected by whole seconds without changing
the offset within the second.  This is unlike the missing value corrections,
which must force a 1-second offset from the previous scan because the
`pps_step` is uncertain and thus the relative offset is uncertain.

### Samples with incorrect pps_step

Sometimes successive scans have consecutive `pps_count` values, but the
samples are more or less than 1 second apart, the `pps_step` value jumps, and
there are dummy values (-9999) in the channel data.

In the hotfilm acquisition code, `pps_step` is set whenever `pps_count`
changes, and it does not actually distinguish changes to and from -9999.  Thus
the expected `pps_step` might appear in the first read of the first
half-second of the sample, at the index where `pps_count` changes.  However,
in the second half of the sample, `pps_step` changes again when `pps_count`
contains -9999, and then again when `pps_count` changes back.  Really the
`pps_count` transition can be lost anywhere in the sample due to buffer
overflows. As long as at least one scan contains the consecutive value for the
count, then the succeeding sample can be assumed to be contiguous.

Here are the log messages from one case where `pps_step` jumps due to dummy
values in the scans:

```
DEBUG:hotfilm.read_hotfilm:handling scan 2023-09-20T18:15:40.326250: 6 variables, 4000 samples/channel, count=35436, step=2695
INFO:hotfilm.read_hotfilm:scan time fixed, from 2023-09-20T18:15:40.326250 to 2023-09-20T18:15:40.843000
WARNING:hotfilm.read_hotfilm:2023-09-20T18:15:40.843000: fixed pps_step from 2695 to 628
INFO:hotfilm.read_hotfilm:scan 2023-09-20T18:15:40.843000, variable ch0[0, 3999], filled 1446 nans at indices: [(1249, 2694)]
```

In the raw data, `pps_step` is 2695, the first index after the dummy values
from [1249, 2694].  The `dump_hotfilm.py` script detects the contiguous
samples despite the errant `pps_step`, so it corrects the sample times and
`pps_step`, and it replaces the dummy values with `NaN`. All of the scans are
kept, including the ones with good values, rather than leaving out the entire
second of data.

Note that only a subset of a buffer can be filled with dummy values.  The ADC
was scanning at 4 kHz and the code was reading 2000 scans per read, so the
first read had missing values at [1249, 1999], and the second read had missing
values at [2000, 2694].

See [Data-Acquisition.md](Data-Acquisition.md) for more information.

### Time coordinate variables and ncdump

The netcdf files use a time coordinate variable with `int64` type and units in
the form `microseconds since 2023-09-20 18:00:00+00:00`.  This ensures the
coordinate does not overflow and can be stored and compared exactly without
concern for floating point rounding or precision errors.

This should conform to the `udunits` conventions, and as such should allow the
`ncdump` program to dump the time coordinates as time strings, but
unfortunately no way is known of making that work. The changes tried so far
include adding a `calendar:standard` attribute, changing `microseconds` to
`us`, and using options like `ncdump -st -fc`.  `udunits2` parses the units
strings fine, so perhaps the problem is the `int64` type.

## Calibration

See [Calibration.md](Calibration.md) for details on converting the hotfilm
voltages to wind speeds.

## Production processing

The production data processing is scripted in the
[run_hotfilm.sh](../run_hotfilm.sh) script.  The script automates multiple
processing steps and also ensures consistent commands, arguments, and file
layouts are used for each run.

The NIDAS and Python environments should be activated before running the
script.  This is an example sequence on mercury:

    source /opt/nidas/bin/setup_nidas.sh
    export ISFS=/h/eol/isfs/isfs
    eval $(/opt/local/miniforge3/bin/mamba shell hook --shell bash)
    mamba activate hotfilm

These commands can be used to set the NIDAS project environment.

    source $ISFS/scripts/isfs_functions.sh
    set_project M2HATS hr_qc_instrument

The `set_project` is not strictly necessary to run `dump_hotfilm.py`, but
it is necessary when the same environment setup is used to generate the
`hr_qc_instrument` sonic dataset which the calibration requires.

It can also be helpful to add the hotfilm source directory to the `PATH`, so
the scripts can be called without paths, eg:

    export PATH=/opt/local/m2hats/hotfilm:${PATH}

The `run_hotfilm.sh` script performs these steps:

 1. Run `dump_hotfilm.py` to convert the raw hotfilm voltage data files to
    netCDF, expecting the raw data to be located in
    `/scr/isfs/projects/M2HATS/raw_data`.  The output files are written to a
    subdirectory called `hotfilm`.
 2. Stage the high-rate sonic data from the source location,
    `/export/flash/isf/isfs/data/M2HATS/20250113/hr_qc_instrument/`, into a
    subdirectory named `hr_qc_instrument`.
 3. Run the `calibrate_hotfilm.py` script against the data in the `hotfilm`
    and `hr_qc_instrument` subdirectories, writing the netCDF output into the
    `windspeed` subdirectory and plots into a subdirectory of the `windspeed`
    output.
 4. Generate `index.html` files for all of the directories, so that all of the
    data files and plots can browsed and downloaded over the web server.

In practice, each production run is done in a new directory.  This keeps all
the runs separate and self-contained.  Since the hotfilm wind speeds depend
critically on the raw hotfilm data and the sonic data, those data are either
generated (hotfilm voltages) or staged (sonic winds) into the same directory,
before the calibrated hotfilm wind speeds are generated.

    cd /scr/isfs/projects/M2HATS
    cd `run_hotfilm.sh create`
    run_hotfilm.sh

When the run output is ready to be released through the web site, these steps
create the web index in the output and link the output to the web filesystem:

    run_hotfilm.sh index
    run_hotfilm.sh publish

If necessary, steps can be run separately.  See the usage info with `-h` or
`help`.

The `run_hotfilm.sh` script processes multiple days in parallel, either the
default set of days or the dates specified on the command line.
