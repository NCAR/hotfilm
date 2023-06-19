# Hot Film Sampling with LabJack

The plan is to use a [Labjack
T7](https://labjack.com/pages/support?doc=%2Fdatasheets%2Ft-series-datasheet%2F)
to sample at least 4 hotfilm AD channels at up to 2 KHz and record the
channels to disk in NIDAS dat archive format.

The program will run separately from the NIDAS `dsm` process, and it will be
built outside of the NIDAS source tree, so it can be built easily on the DSM3
Pi against buster branch but also on eol servers against nidas master branch.

## LabJack T7 C++ interface

The recommended library interface for the T7 is the LJM C/C++ library:

- [Software info.](https://labjack.com/pages/support?doc=%2Fsoftware-driver%2Fexample-codewrappers%2Fcc-for-ljm-windows-mac-linux%2F).
- [Download the beta examples](https://cdn.docsie.io/file/workspace_u4AEu22YJT50zKF8J/doc_VDWGWsJAhd453cYSI/boo_KXghxr3Yqvg6QlfuD/file_OaxHN7aj5f8twxWoh/c_c_ljm_2021-08-20_0.zip)


## LabJack T7 Python

The T7 uses a different python library from the [LabJack U6](LabJackU6.md),
apparently a wrapper to the LJM C library, called `labjack-ljm-python`.  It
can be installed with pip.

```
pip install labjack-ljm
```
