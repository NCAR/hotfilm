import time
import sys
from functools import partial
from threading import Thread
from random import random
from bokeh.models import ColumnDataSource
from bokeh.plotting import curdoc, figure
from bokeh.layouts import layout
from bokeh.models import Div, Spinner, Dropdown

import numpy as np

import dump_hotfilm as dhf


def sinewave(x, amp=1.0, freq=1.0, phase=0.0, offset=0.0):
    angles = x * 2 * np.pi * freq - phase
    return amp * np.sin(angles) + offset


# only modify from a Bokeh session callback
source = ColumnDataSource(data=dict(x=[0], y=[0]))

# This is important! Save curdoc() to make sure all threads
# see the same document.
doc = curdoc()


async def update(x, y):
    # source.stream(dict(x=x, y=y))
    source.data = dict(x=x, y=y)


def blocking_task():
    while True:
        # do some blocking computation
        time.sleep(1)
        x = np.arange(0, 1, 1.0/2000)
        freq = (50 + random()*100)
        y = sinewave(x, 1, freq, 0, 2)
        print(x)
        print(y)

        # but update the document from a callback
        doc.add_next_tick_callback(partial(update, x=x, y=y))


hf = dhf.ReadHotfilm()
if len(sys.argv) > 1:
    hf.set_source(sys.argv[1])


def read_hotfilm():
    hf.start()
    while True:
        y = hf.get_data()
        if y is None:
            break
        # but update the document from a callback
        x = np.arange(0, 1, 1.0/len(y))
        doc.add_next_tick_callback(partial(update, x=x, y=y))


plot = figure(height=600, width=1000, title="Hotfilm A/D Channels",
              x_range=[0, 1], y_range=[-1, 10])
plot.line(x='x', y='y', source=source, line_width=2)


div = Div(
    text="""
        <p>Select channel 0-3:</p>
        """,
    width=200,
    height=30,
)

spinner = Spinner(
    title="Channel",  # a string to display above the widget
    low=0,  # the lowest possible number to pick
    high=3,  # the highest possible number to pick
    step=1,  # the increments by which the number can be adjusted
    value=0,  # the initial value to display in the widget
    width=200,  # the width of the widget in pixels
    )


dropdown = Dropdown(label="Time",
                    menu=[("Time", "time"),
                          ("Frequency", "frequency")])


def update_channel(attrname, old, new):
    hf.select_channel(new)


def select_domain(domain):
    if domain == "time":
        hf.spectrum = False
        dropdown.update(label="Time")
    else:
        hf.spectrum = True
        dropdown.update(label="Frequency")


spinner.on_change('value', update_channel)
dropdown.on_click(lambda event: select_domain(event.item))


layout = layout([
    [div, spinner, dropdown],
    [plot],
])

doc.add_root(layout)

# thread = Thread(target=blocking_task)
thread = Thread(target=read_hotfilm)
thread.start()
