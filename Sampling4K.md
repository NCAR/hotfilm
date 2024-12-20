# Sampling at 4 KHz

During the M2HATS project, the sample rate was tested at 4 KHz and then
switched permanently for the rest of the project.  These are notes about that
process.

## Initial Tests

This file was the first test with 4K samples, 4 channels, res index 3:

    /data/isfs/projects/M2HATS/raw_data/hotfilm_20230906_192000.dat

Sampling switched back to 2KHz operation with this file:

    /data/isfs/projects/M2HATS/raw_data/hotfilm_20230906_195015.dat

## Troubleshooting continuous 4K sampling

Started 4K scan rate with 4 hz read rate, and this time with setcap binary:

    hotfilm --xml hotfilm.xml --scanrate 4000 --readrate 4 --log info --resolution 3

    /data/isfs/projects/M2HATS/raw_data/hotfilm_20230907_173454.dat

No hiccups noticed, so that was run for a while from systemd unit beginning
with this file:

    /data/isfs/projects/M2HATS/raw_data/hotfilm_20230907_174522.dat

Timing problems show up in these files:

    hotfilm_20230907_180000.dat
    hotfilm_20230907_190000.dat
    hotfilm_20230907_200000.dat
    hotfilm_20230907_210000.dat

Started running 4K on t0t at 4 reads/sec with this file:

    /media/usbdisk/projects/M2HATS/raw_data/hotfilm_20230907_213933.dat

After moving to t0t and running at 4K, there are still some dropouts, but fewer:

    daq@t0t:~ $ journalctl --user -u hotfilm | grep dummy
    Journal file /var/log/journal/4e4c8ef05b1849f6b7ed54bd5f6eccde/user-1001@00047e7eab4e2fd8-7fe7087d55aa3583.journal~ is truncated, ignoring file.
    Sep 07 23:28:38 t0t bash[21092]: 2023-09-07,23:28:38|ERROR|dummy value -9999 in pps_count at scan 2873: scans were dropped!
    Sep 08 00:45:57 t0t bash[21092]: 2023-09-08,00:45:57|ERROR|dummy value -9999 in pps_count at scan 3860: scans were dropped!
    Sep 08 05:46:16 t0t bash[21092]: 2023-09-08,05:46:16|ERROR|dummy value -9999 in pps_count at scan 1818: scans were dropped!
    Sep 08 09:29:43 t0t bash[21092]: 2023-09-08,09:29:43|ERROR|dummy value -9999 in pps_count at scan 3303: scans were dropped!
    Sep 08 13:43:12 t0t bash[21092]: 2023-09-08,13:43:12|ERROR|dummy value -9999 in pps_count at scan 136: scans were dropped!
    Sep 08 14:59:32 t0t bash[21092]: 2023-09-08,14:59:32|ERROR|dummy value -9999 in pps_count at scan 2813: scans were dropped!
    Sep 08 16:11:34 t0t bash[21092]: 2023-09-08,16:11:34|ERROR|dummy value -9999 in pps_count at scan 1754: scans were dropped!

Starting with this file:

    /media/usbdisk/projects/M2HATS/raw_data/hotfilm_20230908_164118.dat

Ran at 4k but with read rate 2 instead of 4, to see if that reduced the
dropouts further, since maybe the 4 reads/sec were not needed when running
close to the labjack.  However, overflows and timestamp jumps still happened.
Presumably between the Pi and the wireless there is not enough throughput to
keep up with the labjack.

    daq@t0t:~ $ journalctl --user -u hotfilm | grep dummy
    Journal file /var/log/journal/4e4c8ef05b1849f6b7ed54bd5f6eccde/user-1001@00047e7eab4e2fd8-7fe7087d55aa3583.journal~ is truncated, ignoring file.
    Sep 07 23:28:38 t0t bash[21092]: 2023-09-07,23:28:38|ERROR|dummy value -9999 in pps_count at scan 2873: scans were dropped!
    Sep 08 00:45:57 t0t bash[21092]: 2023-09-08,00:45:57|ERROR|dummy value -9999 in pps_count at scan 3860: scans were dropped!
    Sep 08 05:46:16 t0t bash[21092]: 2023-09-08,05:46:16|ERROR|dummy value -9999 in pps_count at scan 1818: scans were dropped!
    Sep 08 09:29:43 t0t bash[21092]: 2023-09-08,09:29:43|ERROR|dummy value -9999 in pps_count at scan 3303: scans were dropped!
    Sep 08 13:43:12 t0t bash[21092]: 2023-09-08,13:43:12|ERROR|dummy value -9999 in pps_count at scan 136: scans were dropped!
    Sep 08 14:59:32 t0t bash[21092]: 2023-09-08,14:59:32|ERROR|dummy value -9999 in pps_count at scan 2813: scans were dropped!
    Sep 08 16:11:34 t0t bash[21092]: 2023-09-08,16:11:34|ERROR|dummy value -9999 in pps_count at scan 1754: scans were dropped!
    Sep 08 18:38:25 t0t bash[6983]: 2023-09-08,18:38:25|ERROR|dummy value -9999 in pps_count at scan 697: scans were dropped!
    Sep 08 21:05:29 t0t bash[6983]: 2023-09-08,21:05:29|ERROR|dummy value -9999 in pps_count at scan 3069: scans were dropped!
    Sep 08 22:25:31 t0t bash[6983]: 2023-09-08,22:25:31|ERROR|dummy value -9999 in pps_count at scan 251: scans were dropped!
    Sep 08 23:39:47 t0t bash[6983]: 2023-09-08,23:39:47|ERROR|dummy value -9999 in pps_count at scan 3217: scans were dropped!
    Sep 09 01:03:50 t0t bash[6983]: 2023-09-09,01:03:50|ERROR|dummy value -9999 in pps_count at scan 3: scans were dropped!

## Diagnostics

These commands can be used to generate some useful summaries of the 2K and 4K transition and performance:

    (for df in hotfilm_2023090[6789]_*.dat; do (set -x; data_stats -P 30 -n 1 $df) ; done) >& /tmp/hotfilm_4k_stats.txt

The 4K channel variables have length 16000, while the 2K channels have length 8000.

This command dumps just the 501 sample with the timing and buffer statistics:

    data_dump --xml /opt/local/m2hats/hotfilm/hotfilm.xml -i 200,501 hotfilm_20230909_220000.dat|& less

When the third variable, `device_scan_backlog`, is sometimes 1, and with the
`timetag_to_system` more than 1 second, that implies the host is lagging
behind the labjack and there could be an error in the timestamp offset as a
result.
