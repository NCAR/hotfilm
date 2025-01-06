# Testing

## Running tests

Tests can be run with `pytest`.  The tests and the code use the python
`logging` package to log messages, so running the tests with log output can be
helpful:

```sh
pytest -s --log-cli-level=debug test_dump_hotfilm.py -k test_netcdf_output
```

The netcdf output test currently requires `nc_compare`, so the test result
will be `XFAIL` if it cannot be found at the path `/opt/local/bin/nc_compare`.

## Generating test data

Some tests generate output from test data files, and the test data files are
excerpts from larger original data files.

For example, this command creates a NIDAS dat file with just 5 minutes of
channel 2 data, without the other channels or the PPS counter channel:

```sh
nidsmerge --samples 200,501 --samples 200,512 --samples 200,522 --end "2023-08-04_18:05:00" -i hotfilm_20230804_180000.dat -o test_data/channel2_20230804_180000_05.dat
```

The `nidsmerge` command above requires the M2HATS reprocessing branch of NIDAS
to get the `--samples` argument.

The baseline netcdf output is then created with `dump_hotfilm.py`:

```sh
./dump_hotfilm.py --netcdf "channel2_%Y%m%d_%H%M%S.nc" --channel 2 test_data/channel2_20230804_180000_05.dat
```

Testing only one channel and 5 minutes saves space in the repository and also
saves time running the tests.

The netcdf input test data is reduced by selecting specific variables with
`ncks`:

```sh
ncks -v u_2m_t0,time isfs_m2hats_qc_geo_tiltcor_hr_20230804_160000.nc u_2m_t0_20230804_160000.nc
```
