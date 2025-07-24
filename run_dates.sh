#! /bin/bash

# make sure right mamba environment and nidas path are active
# mamba activate hotfilm
# source /opt/nidas/bin/setup_nidas.sh

# Output will go into subdirectories of the current directory by default.

hotfilmdir=/opt/local/m2hats/hotfilm
dataroot=/scr/isfs/projects/M2HATS
rawdata=${dataroot}/raw_data
sonics=/export/flash/isf/isfs/data/M2HATS/20250113/hr_qc_instrument/
local_sonics="hr_qc_instrument"
speed_spec=hotfilm_wind_speed_%Y%m%d_%H%M%S.nc
plot_spec=hotfilm_calibrations_%Y%m%d_%H%M%S.png

hotfilm_output=./hotfilm
hotfilm_output_spec=${hotfilm_output}/hotfilm_%Y%m%d_%H%M%S.nc
calibrated_output=./windspeed/$date
calibrated_output_spec=${calibrated_output}/hotfilm_wind_speed_%Y%m%d_%H%M%S.nc
plot_output=./windspeed/$date/plots
plot_output_spec=${plot_output}/hotfilm_calibrations_%Y%m%d_%H%M%S.png

dumphotfilm="${hotfilmdir}/dump_hotfilm.py --log info"
calibrate="${hotfilmdir}/calibrate_hotfilm.py --log info --plot --calibrate"

do_dump=0
do_stage=0
do_calibrate=0
dates=""

# Default dates
get_dates() {
cat <<EOF
20230803
20230804
20230805
20230827
20230905
20230915
20230919
20230920
20230921
20230922
EOF
}


run_dump() # date
{
    date="$1"
    echo "Dumping hotfilm data for $date ..."
    set -x
    mkdir -p ${hotfilm_output}
    $dumphotfilm --netcdf ${hotfilm_output_spec} \
        ${rawdata}/hotfilm_${date}_*.dat
}


stage_sonic_data() # date
{
    date="$1"
    echo "Staging sonic data for $date ..."
    # make sure corresponding sonic data files are copied into this run
    # directory.  I'm not sure if reading the sonic data from the local
    # copy is any slower than reading directly from the flash filesystem,
    # but this at least makes the runs more self-contained, and this
    # includes the sonic data in the final output when linked to the web
    # filesystem.
    mkdir -p "${local_sonics}"
    (set -x; rsync -av $sonics/isfs_m2hats_qc_hr_inst_${date}_*.nc \
     "${local_sonics}/")
}


run_calibrate() # date
{
    date="$1"
    stage_sonic_data $date
    echo "Calibrating hotfilm data for $date ..."
    calibrated_output=./windspeed/$date
    calibrated_output_spec=${calibrated_output}/${speed_spec}
    plot_output=./windspeed/$date/plots
    plot_output_spec=${plot_output}/${plot_spec}

    set -x
    mkdir -p ${calibrated_output}
    mkdir -p ${plot_output}
    $calibrate --netcdf ${calibrated_output_spec} \
        --images ${plot_output_spec} \
        --sonics ${local_sonics} \
        ${hotfilm_output}/hotfilm_${date}_*.nc
}


run_date() # date
{
    date="$1"
    echo "Running date: $date"
    if [ $do_dump -ne 0 ] ; then
        run_dump $date
    fi
    if [ $do_stage -ne 0 ] ; then
        stage_sonic_data $date
    fi
    if [ $do_calibrate -ne 0 ] ; then
        run_calibrate $date
    fi
}


index_dirs() {
    pwd > index-dirs.txt
    (set -x; python ~isfs/webIndex.py index-dirs.txt)
}


while [[ $# -gt 0 ]] ; do
    case "$1" in
        --dump)
            do_dump=1
            shift
            ;;
        --calibrate)
            do_calibrate=1
            shift
            ;;
        --stage)
            do_stage=1
            shift
            ;;
        --dates)
            get_dates
            exit 0
            ;;
        --index)
            index_dirs
            exit 0
            ;;
        *)
            if [[ -z "$dates" ]] ; then
                dates="$1"
            else
                dates="$dates $1"
            fi
            shift
            ;;
    esac
done

if [ $do_dump -eq 0 ] && [ $do_calibrate -eq 0 ] && [ $do_stage -eq 0 ]; then
    do_dump=1
    do_calibrate=1
fi

if [ -z "$dates" ]; then
    dates=$(get_dates)
fi

for day in $dates ; do
    run_date $day >& run_dates.${day}.log &
done

wait
