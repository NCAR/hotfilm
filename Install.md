
# Installation

## LabJack T7 C++ interface

The recommended library interface for the T7 is the LJM C/C++ library:

- [Software library](https://labjack.com/pages/support?doc=/software-driver/installer-downloads/ljm-software-installers-t4-t7-digit/)
- [Software examples](https://labjack.com/pages/support?doc=%2Fsoftware-driver%2Fexample-codewrappers%2Fcc-for-ljm-windows-mac-linux%2F).

The `hotfilm` program started out from one of the stream examples.

## Install LabJackM library on DSM3

The example code had a call to
[LJM_GetStreamTCPReceiveBufferStatus()](https://labjack.com/pages/support?doc=%2Fsoftware-driver%2Fljm-users-guide%2Fgetstreamtcpreceivebufferstatus%2F),
but that is only in LJM 1.2 or later.  The _official_ LJM version for Raspbian
linux armhf is only up to LJM 1.1804.

Instead, download the [beta armv7
package](https://cdn.docsie.io/file/workspace_u4AEu22YJT50zKF8J/doc_VDWGWsJAhd453cYSI/boo_9BFzMKFachlhscG9Z/file_hI7tX3BoVMbpqFkXI/labjack_ljm_minimal_2020_03_31_armhf_betatar.gz).
Extract the archive and follow the instructions to install.

## Installing NIDAS build dependencies

These commands can be used to install NIDAS build dependencies, to build the `hotfilm` program against
NIDAS on the DSM:

```plain
apt-get update
apt-get install -y nidas nidas-libs nidas-dev
apt-get install -y --no-install-recommends gnupg wget cmake apt-utils \
    sudo vim nano curl git ca-certificates build-essential fakeroot \
    libncurses-dev bc dh-make quilt rsync flex libfl-dev gawk devscripts pkg-config libbz2-dev \
    libgsl0-dev libcap-dev libxerces-c-dev libbluetooth-dev libnetcdf-dev reprepro \
    libjsoncpp-dev lsb-release xsltproc docbook-xsl libxml2-dev libi2c-dev valgrind net-tools less
apt install xmlrpc++-dev
pip3 install scons
mkdir -p ~/.scons/site_scons
(cd ~/.scons/site_scons && git clone https://github.com/NCAR/eol_scons)
```

At the moment, it's likely that the DSM running `hotfilm` needs to be newer
than the latest NIDAS package version.  In that case, build the [buster
branch](https://github.com/ncareol/nidas/tree/buster) in a container and rsync
to the DSM, using [these
instructions](https://github.com/ncareol/nidas/blob/buster/Develop_Pi.md).

## Building hotfilm

Build the `hotfilm` program with `scons`:

```plain
daq@dsm214:~/hotfilm $ scons -Q NIDAS_PATH=/opt/nidas-dev
g++ -o hotfilm.o -c -std=c++11 -Wno-deprecated -fpic -fPIC -rdynamic -g -Wall -O2 -I. -I/opt/nidas-dev/include -I/usr/include/xmlrpcpp hotfilm.cc
g++ -o hotfilm -fpic -fPIC -rdynamic -Wl,-rpath=/opt/nidas-dev/lib hotfilm.o -L/opt/nidas-dev/lib -lnidas -lnidas_dynld -lnidas_util -lxerces-c -lxmlrpcpp -lLabJackM
```

## Installing hotfilm

The `hotfilm` program can be installed into the `bin` directory under `NIDAS_PATH`
using `scons install`:

```plain
$ scons -Q install
Install file: "hotfilm" as "/opt/local/nidas-buster/bin/hotfilm"
```

There is a separate scons target which can set the capabilities on the
installed program which allow it to change the scheduling policy:

```plain
$ sudo scons -Q setcap
/usr/sbin/setcap cap_net_admin,cap_sys_nice=pe /opt/local/nidas-buster/bin/hotfilm
```

If the program has to be installed into a directory owned by root, then both
the install and the setcap can be done with the `install.root` target:

```plain
$ scons -Q
g++ -o hotfilm.o -c -std=c++11 -Wno-deprecated -fpic -fPIC -rdynamic -g -Wall -O2 -I. -I/opt/local/nidas-buster/include -I/usr/include/xmlrpcpp hotfilm.cc
g++ -o hotfilm -fpic -fPIC -rdynamic -Wl,-rpath=/opt/local/nidas-buster/lib64 hotfilm.o -L/opt/local/nidas-buster/lib64 -lnidas -lnidas_dynld -lnidas_util -lxerces-c -lxmlrpcpp -lLabJackM
$ sudo scons -Q --site-dir=/home/daq/.scons/site_scons install.root
Install file: "hotfilm" as "/opt/local/nidas-buster/bin/hotfilm"
/usr/sbin/setcap cap_net_admin,cap_sys_nice=pe /opt/local/nidas-buster/bin/hotfilm
```

## Configure the LabJack for the network

According to the LabJack T-series documentation, the throughput is a little
better over the network than over the USB.

Use the Kipling software to configure the ethernet address to 192.168.1.190.
No gateway or DNS settings are needed because the LabJack should not need to
connect anywhere except the local subnet.  Since Kipling does not run on the
Pi, it has to be run from somewhere else.

After testing on a laptop, running the same software on a DSM failed to find
the LabJack.  The open call does not allow the IP address to be specified;
instead, it looks in a file which caches recent IP addresses, or else it
relies on the LabJack to reply to UDP broadcast.  I suppose the DSM firewall
might have prevented replies from the LabJack, causing the open to fail.
Either way, a workaround is to copy a working IP cache file to the DSM.  There
is a copy of that file in this repository for a LabJack at IP address
192.168.1.190.

```plain
cp ljm_auto_ips.json /usr/local/share/LabJack/LJM/ljm_auto_ips.json
```

In one instance while testing the LabJack, it could not be opened by LJM and
it failed to respond to pings on the local network.  Cycling the power
restored network connectivity.  Thus it would be a good idea to power the
LabJack through one of the 5V power relays on the DSM3, so the power can be
cycled remotely.

## Initial setup and USB

These are the initial steps used to attach to a LabJack T7 over USB and try it
out with some tutorials on a Fedora Linux laptop.

```plain
extract labjack_ljm_software_2020_03_30_x86_64_betatar.gz
run installer
plug in labjack
dmesg | tail
[79519.779418] usb 1-1: new full-speed USB device number 7 using xhci_hcd
[79519.907929] usb 1-1: New USB device found, idVendor=0cd5, idProduct=0007, bcdDevice= 0.00
[79519.907942] usb 1-1: New USB device strings: Mfr=1, Product=2, SerialNumber=3
[79519.907948] usb 1-1: Product: LabJack T7
[79519.907952] usb 1-1: Manufacturer: LabJack LLC
```

The Kipling software fails unless `GConf2-devel` is installed.

```plain
dnf install GConf2-devel
/opt/labjack_kipling/Kipling
```

There is a helpful [quick start
tutorial](https://labjack.com/pages/support?doc=/quickstart/t7-quickstart-tutorial-platinum/),
and the Kipling interface provides a nice pinout diagram and a way to test
different register settings.


## LabJack T7 Python

The T7 uses a different python library from the [LabJack U6](LabJackU6.md),
apparently a wrapper to the LJM C library, called `labjack-ljm-python`.  It
can be installed with pip.

```plain
pip install labjack-ljm
```
