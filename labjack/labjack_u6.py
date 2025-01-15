
import logging
import time
import datetime as dt

import u6


logger = logging.getLogger(__file__)


def summarize(ain):
    """
    Generate simple string summarizing the array of AD samples.
    """
    nsamples = len(ain)
    nmissing = len([x for x in ain if x == -9999.0])
    avg = sum(ain) / nsamples if nsamples else -9999.0
    return f"n={nsamples},nmissing={nmissing},avg={avg:.2f}"

def main():
    logging.basicConfig(level=logging.INFO)

    d = u6.U6()
    # Make sure the stream has been stopped before reconfiguring and starting
    # streaming again.  This seems to be the most reliable way to make sure
    # the labjack is in a stopped default state and is ready to be opened and
    # configured.  This avoids errors trying to start or stop a device which
    # is already started or already stopped.
    d.reset()
    d.close()
    time.sleep(5)

    d = u6.U6()

    logger.info("opened U6: %s", repr(d.configU6()))

    # Setup streaming.  The stream config can either specify ScanFrequency or
    # it can set all the clocking parameters explicitly.  The buffer size is
    # 984 samples, or 246 scans, but the most samples that can be read in a
    # single packet is 25.  (The SamplesPerPacket setting can be 1-25.)  High
    # scan rates require the highest samples per packet.
    #
    # See
    # https://labjack.com/support/datasheets/u6/low-level-function-reference/streamconfig.

    if d.streamStarted:
        d.streamStop()
    nchannels = 4
    d.streamConfig(NumChannels=nchannels,
                   ResolutionIndex=0,
                   ChannelNumbers=list(range(nchannels)),
                   ChannelOptions=[0]*nchannels,
                   ScanFrequency=2000)
    d.getCalibrationData()

    nscans = 0
    start = dt.datetime.now(tz=dt.timezone.utc)
    d.streamStart()

    end = start
    try:
        for data in d.streamData(convert=False):
            end = dt.datetime.now(tz=dt.timezone.utc)
            # type(data['result']) is <class 'bytes'> on python3
            nbytes = len(data['result'])
            bpp = nbytes / data['numPackets']
            logger.info("read: errors=%d, numPackets=%d, missed=%d, firstPacket=%d, bytes=%d, bpp=%g",
                        data['errors'], data['numPackets'], data['missed'], data['firstPacket'],
                        nbytes, bpp)
            reading = d.processStreamData(data['result'])
            nscans += len(reading['AIN0'])
            logger.info("ain0: %s, ain1: %s, ain2: %s, ain3: %s",
                        summarize(reading['AIN0']),
                        summarize(reading['AIN1']),
                        summarize(reading['AIN2']),
                        summarize(reading['AIN3']))
            # print(repr(reading))

    except KeyboardInterrupt:
        logger.info("interrupted...")
        d.streamStop()

    seconds = (end - start).total_seconds()
    scan_rate = nscans / seconds if seconds else 0.0
    logger.info("%d scans in %.2f seconds; scan rate: %.2f Hz",
                nscans, seconds, scan_rate)


if __name__ == "__main__":
    main()
