#! /bin/env python

import sys
import argparse
import logging
import numpy as np
from hotfilm.utils import iso_to_datetime64
from typing import Optional, List

from hotfilm.read_hotfilm import ReadHotfilm

logger = logging.getLogger(__name__)


def apply_args(hf: ReadHotfilm, argv: Optional[List[str]]):
    parser = argparse.ArgumentParser(
        formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    parser.add_argument("input", nargs="+",
                        help="1 or more data files, or a sample "
                        "server specifier, like sock:t0t:31000.",
                        default=None)
    parser.add_argument("--channel", action="append", dest="channels",
                        default=None,
                        help="Channels from 0-3, or all by default")
    parser.add_argument("--begin",
                        help="Output scans after begin, in ISO UTC format.")
    parser.add_argument("--end",
                        help="Output scans up until end, in ISO UTC format.")
    parser.add_argument("--delay", type=float,
                        help="Wait DELAY seconds between returning scans.  "
                        "Useful for simulating real-time data when called "
                        "from the web plotting app with data files.",
                        default=0)
    parser.add_argument("--keep-contiguous", action="store_true",
                        help="Adjust sample times in contiguous blocks to "
                        "keep them exactly at the nominal sample rate, "
                        "even when the labjack clock drifts relative to GPS.")
    minminutes = np.timedelta64(hf.minblock, 'm').astype(int)
    parser.add_argument("--min", type=int, default=minminutes,
                        help="Minimum minutes to write into a file.")
    maxminutes = np.timedelta64(hf.maxblock, 'm').astype(int)
    parser.add_argument("--max", type=int, default=maxminutes,
                        help="Maximum minutes to write into a file.  "
                        "If zero, the only limit is set by --interval.")
    interval_minutes = np.timedelta64(hf.file_interval, 'm').astype(int)
    parser.add_argument("--interval", type=int, default=interval_minutes,
                        metavar="MIN",
                        help="Start netcdf files at intervals of MIN minutes.")
    parser.add_argument("--netcdf", help="Write data to named netcdf file")
    parser.add_argument("--text", help="Write data in text columns to file.  "
                        "Filenames can include time specifiers, "
                        "like %%Y%%m%%d_%%H%%M%%S.")
    parser.add_argument("--timeformat",
                        help="Timestamp format, iso or %% spec pattern.  "
                        "Use %%s.%%f for "
                        "floating point seconds since epoch.",
                        default=hf.timeformat)
    parser.add_argument("--log", choices=['debug', 'info', 'error'],
                        default='info')
    args = parser.parse_args(argv)

    if not args.text and not args.netcdf:
        parser.error("Specify output with either --text or --netcdf.")

    logging.basicConfig(level=logging.getLevelName(args.log.upper()))
    hf.set_source(args.input)
    if args.channels:
        hf.select_channels(args.channels)
    hf.set_min_max_block_minutes(args.min, args.max)
    hf.file_interval = np.timedelta64(args.interval, 'm')
    if args.begin:
        hf.begin = iso_to_datetime64(args.begin)
    if args.end:
        hf.end = iso_to_datetime64(args.end)

    hf.set_time_format(args.timeformat)
    hf.delay = args.delay
    hf.keep_contiguous = args.keep_contiguous
    return args


def main(argv: List[str]):
    hf = ReadHotfilm()
    args = apply_args(hf, argv[1:])
    # record the command line arguments for the history attribute
    hf.set_command_line(argv)
    hf.start()
    # netcdf takes precedence over text default
    if args.netcdf:
        hf.write_netcdf_file(args.netcdf)
    elif args.text:
        hf.write_text_file(args.text)
    else:
        hf.write_text(sys.stdout)
    if hf.unmatched_lines > 0:
        logger.warning("%d unmatched lines in data_dump output.",
                       hf.unmatched_lines)


if __name__ == "__main__":
    main(sys.argv)
