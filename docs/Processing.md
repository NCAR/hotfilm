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
