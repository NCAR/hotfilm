# Hot film data processing

These are notes about reading the raw data files and converting them to other
output formats, possibly with calibrations to wind speed.

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

## Exporting to netcdf

The [dump_hotfilm.py](dump_hotfilm.py) script can also write netcdf using the
`--netcdf` option.  So far it writes a very minimal netcdf file almost exactly
equivalent to the CSV text output.  In particular, the files break at the same
places, when there is an interruption in the data stream or the maximum file
length is reached.

Below are the steps used to generate the first draft of netcdf output on
mercury.  The first source sets up a conda environment with the python
requirements.  The next two commands replace the m2hats nidas branch with the
install of the nidas buster branch.

    source /opt/local/m2hats/setup_m2hats.sh
    snunset /opt/local/m2hats/nidas
    snset /opt/local/m2hats/nidas-buster
    cd /scr/isfs/projects/M2HATS/netcdf/hotfilm.20241219
    time python /opt/local/m2hats/hotfilm/dump_hotfilm.py --netcdf hotfilm_volts_%Y%m%d_%H%M%S.nc --log info ../../raw_data/hotfilm_20230905_*.dat >& dump.log &

It took about 35 minutes on mercury to convert all of the 2K raw hotfilm data
files for 2023-09-05 to netcdf.

## Test data

There are excerpts of ISFS sonic data and hot film data stored in this
repository, generated as follows:

```sh
cd tests/test_data
ncks -d time,1800,2099 -v u_0_5m_t0,v_0_5m_t0,w_0_5m_t0,u_1m_t0,v_1m_t0,w_1m_t0,u_2m_t0,v_2m_t0,w_2m_t0,u_4m_t0,v_4m_t0,w_4m_t0 .../hr_qc_instrument/isfs_m2hats_qc_hr_inst_20230804_180000.nc isfs_m2hats_qc_hr_inst_uvw_20230804_183000.nc
ncks -d time,0,700000 .../hotfilm_20230804_182917_120.nc hotfilm_20230804_182917_6.nc
```

## Generating and plotting calibrations

The `calibrate_hotfilm.py` script can be used to fit and plot calibrations for
the hot film voltages given ISFS sonic data for the same time period.  The
example below uses the test data mentioned above to generate one calibration
over a 5-minute period for each height:

```sh
./calibrate_hotfilm.py tests/test_data/hotfilm_20230804_182917_6.nc tests/test_data/isfs_m2hats_qc_hr_inst_uvw_20230804_183000.nc --plot
```

This command writes the calibrated hot film wind speeds to netcdf:

```sh
./calibrate_hotfilm.py tests/test_data/hotfilm_20230804_182917_6.nc tests/test_data/isfs_m2hats_qc_hr_inst_uvw_20230804_183000.nc --netcdf hotfilm_wind_speed_%Y%m%d_%H%M%S.nc
```

The netcdf output contains a wind speed variable for each hotfilm channel,
with a name of the form `spdhf_{height}_{site}`.  There are two time
dimensions.  The hotfilm wind speeds use a dimension named `time` with a time
coordinate variable `time`, in units of microseconds since some reference
time.  The interval between `time` coordinates corresponds to the frequency of
the hotfilm voltage sampling.

There is a second time dimension and coordinate variable named
`calibration_time`.  Those time coordinates are intervals corresponding to the
calibration period, usually 5 minutes.  Calibration parameters are attached to
the `calibration_time` coordinate variable.  An example is shown below:

```
int calibration_time(calibration_time) ;
        calibration_time:long_name = "Calibration period begin time" ;
        calibration_time:period_seconds = 300 ;
        calibration_time:mean_interval_seconds = 1 ;
        calibration_time:units = "microseconds since 2023-08-04 18:30:00+00:00" ;
```

The calibration coefficients are in variables named `a_{channel}` and
`b_{channel}`, one value for each variable at each `calibration_time`.  Each
`spdhf` variable has an attribute `hotfilm_channel` to map from that variable
to the `a` and `b` coefficients which were used to calculate that wind speed
during the corresponding calibration period.
