= LabJack U6 =

The LabJack U6 connected to fl1 USB shows up like this:

```sh
daq@fl1:~ $ lsusb
...
Bus 001 Device 011: ID 0cd5:0006 LabJack Corporation 
```

Installed LabJackPython for python3 on fl1:

```sh
daq@fl1:~/LabJackPython $ python3
Python 3.7.3 (default, Jan 22 2021, 20:04:44) 
[GCC 8.3.0] on linux
Type "help", "copyright", "credits" or "license" for more information.
>>> import u6
>>> d = u6.U6()
>>> d.config
d.configIO(          d.configTimerClock(  d.configU6(          
>>> d.configU6()
{'FirmwareVersion': '1.43', 'BootloaderVersion': '6.15', 'HardwareVersion': '2.00', 'SerialNumber': 360013841, 'ProductID': 6, 'LocalID': 1, 'VersionInfo': 4, 'DeviceName': 'U6'}
>>> 
```

Firmware version for this labjack is 1.43, while latest on
https://labjack.com/support/firmware/u6 is 1.45.  Firmware has to be
upgraded from Windows, but upgrade probably isn't needed.
https://labjack.com/support/software/applications/ud-series/ljselfupgrade

This page:

 - https://labjack.com/support/software/installers/exodriver

suggests using the LJM Library for C/C++ for the T7 and T4, or the ljacklm for the U12,
rather than the lower-level exodriver (liblabjackusb).

 - https://labjack.com/support/faq/what-driverlibrary-should-i-use-my-labjack

I think we want "stream mode":

 - https://labjack.com/support/app-notes/maximum-command-response
 - https://labjack.com/support/datasheets/u6/operation/stream-mode

According to this, resolution index 1 (same as 0) is 16 bits, which should be
good enough.  The hot film anemometers range from 0-5V, so we would use the
gain setting of 1 for range +-10V.

 - https://labjack.com/support/datasheets/u6/appendix-b

According to the stream mode table, the U6 can scan 4 channels at 12.5 KHz,
for resolution index 1, gain 1, range +-10V.  It sounds like all four channels
are scanned together, so that means we can reference all channels to the same
time.

The stream buffer size is 984 16-bit samples, so the buffer can hold 984/4=246
4-channel scans.  That means the buffer has to be read at least every 246/2KHz
seconds, or .1 seconds.  However, I'm not sure the LabJackPython API allows
the amount to be read to be changed.

Perhaps there are logic and features in the Diamond and NCAR A/D code in
NIDAS which can be used for the LabJack, especially for optimizing buffer
levels and reporting on dropouts.

LabJack stream mode says it will fill in samples with -9999 if the buffer
overflows.  I'm guessing that happens on the host side software side?  It
does not overwrite a full buffer, it will discard samples while the buffer
is full.

In the simple test script, every read returns at most 1200 samples.  It
returns 1200 for one channel, or 600 for each of two channels, 400 for each of
3, or 300 for each of 4.  I don't know why I was seeing the incomplete before,
unless it had something to do with the scan frequency.  Maybe 10 Hz was too
slow to fill the packet with all complete scans.

1200 samples requires 2*1200 + 14 = 2414 bytes, but the python script is
reporting 3072 bytes of raw data in the read, for 48 packets, or 64 bytes per
packet.  4 channels \* 2 bytes/channel \* 8 scans is 64 bytes, 32 samples, but
the number of values processed from each channel is 300.

```sh
INFO:hotfilm.py:read: errors=0, numPackets=48, missed=0, firstPacket=16, bytes=3072, bpp=64
INFO:hotfilm.py:ain0: n=300,nmissing=0,avg=-10.59, ain1: n=300,nmissing=0,avg=-10.59, ain2: n=300,nmissing=0,avg=-10.59, ain3: n=300,nmissing=0,avg=-10.59
INFO:hotfilm.py:read: errors=0, numPackets=48, missed=0, firstPacket=64, bytes=3072, bpp=64
INFO:hotfilm.py:ain0: n=300,nmissing=0,avg=-10.59, ain1: n=300,nmissing=0,avg=-10.59, ain2: n=300,nmissing=0,avg=-10.59, ain3: n=300,nmissing=0,avg=-10.59
INFO:hotfilm.py:read: errors=0, numPackets=48, missed=0, firstPacket=112, bytes=3072, bpp=64
INFO:hotfilm.py:ain0: n=300,nmissing=0,avg=-10.59, ain1: n=300,nmissing=0,avg=-10.59, ain2: n=300,nmissing=0,avg=-10.59, ain3: n=300,nmissing=0,avg=-10.59
```

bpp is 64, which is `14+(stream_samples_per_packet*2)` according to
`processStreamData()`, so that makes sense since samples_per_packet is 25.
25*48 = 1200.
