import sys
from functools import partial
from threading import Thread
from bokeh.models import ColumnDataSource
from bokeh.plotting import curdoc, figure
from bokeh.layouts import layout
from bokeh.models import Spinner
import logging

import numpy as np

import dump_hotfilm as dhf


logger = logging.getLogger(__name__)

logging.basicConfig(level=logging.DEBUG)


def sinewave(x, amp=1.0, freq=1.0, phase=0.0, offset=0.0):
    angles = x * 2 * np.pi * freq - phase
    return amp * np.sin(angles) + offset


# only modify from a Bokeh session callback
timesource = ColumnDataSource(data=dict(x=[0], y=[0]))
specsource = ColumnDataSource(data=dict(x=[0], y=[0]))

# This is important! Save curdoc() to make sure all threads
# see the same document.
doc = curdoc()


class HotFilmPlot:

    def __init__(self, doc):
        self.spectrum = False
        self.hf = dhf.ReadHotfilm()
        self.doc = doc
        self.hf.select_channels([0])

    def read_hotfilm(self):
        self.hf.start()
        while True:
            data = self.hf.get_data()
            if data.columns[0] not in self.hf.channels:
                continue
            if data is None:
                break
            x = data.index
            channel = data.columns[0]
            y = data[channel]
            # but update the document from a callback
            self.doc.add_next_tick_callback(partial(update, x=x, y=y))

            sy = np.abs(np.fft.rfft(y))**2
            # x = np.arange(0, 1, 1.0/len(y))
            sx = np.fft.rfftfreq(len(y), 1.0/len(y))
            self.doc.add_next_tick_callback(partial(update_spectra, x=sx, y=sy))

    def update_channel(self, attrname, old, new):
        self.hf.select_channels([new])


hfp = HotFilmPlot(doc)
if len(sys.argv) > 1:
    hfp.hf.set_source(sys.argv[1])


async def update(x, y):
    # source.stream(dict(x=x, y=y))
    timesource.data = dict(x=x, y=y)


async def update_spectra(x, y):
    # source.stream(dict(x=x, y=y))
    specsource.data = dict(x=x, y=y)


tplot = figure(height=400, width=1000, title="Hotfilm Channel Voltage",
               x_axis_type="datetime", y_range=[1, 5])
splot = figure(height=400, width=1000, title="Hotfilm Channel Spectrum",
               x_axis_label="Frequency (Hz)",
               x_axis_type="log", y_axis_type="log",
               y_range=[10**-5, 10**5])
tplot.line(x='x', y='y', source=timesource, line_width=2)
splot.line(x='x', y='y', source=specsource, line_width=2)

spinner = Spinner(
    title="Channel",  # a string to display above the widget
    low=0,  # the lowest possible number to pick
    high=3,  # the highest possible number to pick
    step=1,  # the increments by which the number can be adjusted
    value=0,  # the initial value to display in the widget
    width=200,  # the width of the widget in pixels
    )

spinner.on_change('value', hfp.update_channel)

layout = layout([
    [spinner],
    [tplot],
    [splot]
])

doc.add_root(layout)

thread = Thread(target=hfp.read_hotfilm)
thread.start()
