#! /bin/bash

# make sure right mamba environment and nidas path are active
# mamba activate hotfilm
# source /opt/nidas/bin/setup_nidas.sh

# Output will go into subdirectories of the current directory by default.

hotfilmdir=/opt/local/m2hats/hotfilm
rawdata=/scr/isfs/projects/M2HATS/raw_data
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

webroot=/net/www/docs/isf/projects/M2HATS/isfs

# if not already on the path, add the default path to the production scripts.
if ! command -v dump_hotfilm.py >/dev/null 2>&1 ; then
    export PATH="${hotfilmdir}:${PATH}"
fi

dumphotfilm="dump_hotfilm.py --log info"
calibrate="calibrate_hotfilm.py --log info --plot --calibrate"

do_dump=0
do_stage=0
do_calibrate=0
dates=""


usage() {
    cat <<EOF
Usage: $0 [--rawdata DIR] [methods ...] [date ...]

  setup       Print bash command to add hotfilm directory to PATH.
              Use it like this: eval \$(path/to/run_hotfilm.sh setup)
  create      Derive and create, if necessary, a run directory based on
              the current date, and print the directory name.
              Use it like this: cd \$(run_hotfilm.sh create)
  dump        Convert raw hotfilm data to netCDF
  calibrate   Calibrate hotfilm netcdf against sonics and write wind speed
  stage       Stage sonic data locally
  dates       Print default dates
  alldates    Print all dates for which there are hotfilm data files
  index       Index output directories for web access and exit
  publish     Link this run output to the web filesystem
  date ...    List of dates to process (YYYYMMDD),
              otherwise process default dates.
  --rawdata DIR
              Use DIR as the location of the raw hotfilm data files, instead
              of the default $rawdata.

Default operations are dump and calibrate, calibrate implies stage.
EOF
}



# Default dates
get_default_dates() {
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


get_all_dates() {
    # Get all dates for which there are hotfilm data files, and print them
    # in YYYYMMDD format.
    ls ${rawdata}/hotfilm_20??????_*.dat | \
        sed -n 's/.*hotfilm_\([0-9]\{8\}\)_.*\.dat/\1/p' | \
        sort -u
}


run_dump() # date
{
    date="$1"
    echo "Dumping hotfilm data for $date ..."
    $dumphotfilm --version
    set -x
    mkdir -p ${hotfilm_output}
    $dumphotfilm --netcdf ${hotfilm_output_spec} \
        ${rawdata}/hotfilm_${date}_*.dat
    set +x
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
    # Use an extension which webIndex.py does not index, and since it is
    # easy to recreate and otherwise not needed, delete it after indexing.
    pwd > index-dirs.tmp
    (set -x; python ~isfs/webIndex.py index-dirs.tmp)
    rm -f index-dirs.tmp
}


publish_link() {
    # Expect to be run from the output directory.
    thisdir=$(realpath .)
    rundate=$(basename $thisdir)
    case "$rundate" in
        hotfilm.20*)
            (cd ${webroot} && ln -sf $thisdir .)
            if [ $? -ne 0 ]; then
                exit 1
            fi
            echo Link created:
            ls -laF ${webroot}/$rundate
            ;;
        *)
            echo "Unexpected directory name: $rundate"
            exit 1
            ;;
    esac
}


while [[ $# -gt 0 ]] ; do
    case "$1" in
        setup)
            target=`realpath "$0"`
            pathdir=`dirname "$target"`
            echo "export PATH=${pathdir}:\$PATH"
            exit 0
            ;;
        create)
            rundate=`date +%Y%m%d`
            subdir=hotfilm.$rundate
            mkdir -p $subdir
            echo $subdir
            exit 0
            ;;
        dump)
            do_dump=1
            shift
            ;;
        calibrate)
            do_calibrate=1
            shift
            ;;
        stage)
            do_stage=1
            shift
            ;;
        dates)
            get_default_dates
            exit 0
            ;;
        alldates)
            get_all_dates
            exit 0
            ;;
        index)
            index_dirs
            exit 0
            ;;
        publish)
            publish_link
            exit 0
            ;;
        --rawdata)
            rawdata="$2"
            shift 2
            ;;
        help|-h)
            usage
            exit 0
            ;;
        20*)
            if [[ -z "$dates" ]] ; then
                dates="$1"
            else
                dates="$dates $1"
            fi
            shift
            ;;
        *)
            echo "Unknown argument: $1"
            usage
            exit 1
            ;;
    esac
done

if [ $do_dump -eq 0 ] && [ $do_calibrate -eq 0 ] && [ $do_stage -eq 0 ]; then
    do_dump=1
    do_calibrate=1
fi

if [ -z "$dates" ]; then
    dates=$(get_default_dates)
fi

for day in $dates ; do
    run_date $day >& run_hotfilm.${day}.log &
done

wait
