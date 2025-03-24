#! /bin/bash

# make sure right mamba environment and nidas path are active
# mamba activate hotfilm
# source /opt/nidas/bin/setup_nidas.sh

# Dates of interest
dates=read <<EOF
20230804
20230827
20230905
20230915
20230919
20230920
20230922
EOF

date=20230804

# Output will go into subdirectories of the current directory by default.

hotfilmdir=/opt/local/m2hats/hotfilm
dataroot=/scr/isfs/projects/M2HATS
rawdata=${dataroot}/raw_data
sonics=/export/flash/isf/isfs/data/M2HATS/20250113/hr_qc_instrument/
speed_spec=hotfilm_wind_speed_%Y%m%d_%H%M%S.nc
plot_spec=hotfilm_calibrations_%Y%m%d_%H%M%S.png

hotfilm_output=./hotfilm
hotfilm_output_spec=${hotfilm_output}/hotfilm_%Y%m%d_%H%M%S.nc
calibrated_output=./windspeed/$date
calibrated_output_spec=${calibrated_output}/hotfilm_wind_speed_%Y%m%d_%H%M%S.nc
plot_output=./windspeed/$date/plots
plot_output_spec=${plot_output}/hotfilm_calibrations_%Y%m%d_%H%M%S.png

dumphotfilm="${hotfilmdir}/dump_hotfilm.py --log info"
calibrate="${hotfilmdir}/calibrate_hotfilm.py --log info --sonics ${sonics} --plot --calibrate"


get_inputs()
{
    echo "$@" | awk '{ print $1; }'
}


run_dump() # date
{
    date="$1"
    set -x
    mkdir -p ${hotfilm_output}
    $dumphotfilm --netcdf ${hotfilm_output_spec} \
        ${rawdata}/hotfilm_${date}_*.dat
}


run_calibrate() # date
{
    date="$1"
    calibrated_output=./windspeed/$date
    calibrated_output_spec=${calibrated_output}/${speed_spec}
    plot_output=./windspeed/$date/plots
    plot_output_spec=${plot_output}/${plot_spec}

    set -x
    mkdir -p ${calibrated_output}
    mkdir -p ${plot_output}
    $calibrate --netcdf ${calibrated_output_spec} \
        --images ${plot_output_spec} \
        ${hotfilm_output}/hotfilm_${date}_*.nc
}


run_date() # date
{
    date="$1"
    run_dump $date
    run_calibrate $date
}

for dt in $dates ; do
    run_date $dt >& run_dates.${date}.log &
done

wait
