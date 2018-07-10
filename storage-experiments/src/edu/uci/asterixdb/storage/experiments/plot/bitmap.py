
import numpy as np
import pandas
import matplotlib as mt
import matplotlib.pyplot as plt
import os
from base import *
from pathlib import PurePath

query_base_path = base_path + 'query/'
filter_base_path = base_path + 'query-filter/'

time_index = 'time'


def plot_options(xvalues, options, ax, xlabel, xlimit, ylimit):
    x = np.arange(len(xvalues))
    numbars = float(len(options))
    i = 0
    barwidth = 0.2
    for option in options:
        ax.bar(x + (i - numbars / 2) * barwidth, option.data, align='edge', label=option.legend, color=option.color, width=barwidth)
        i += 1
    ax.set_xlabel(xlabel)
    ax.set_xticks(x)
    ax.set_xticklabels(xvalues)
    ax.set_ylim(0, ylimit)


def plot_shared_bitmap(xvalues, options_1, options_2, options_3, output, xlabels, ylabel='Repair Time (s)', xlimit=110, ylimit=1100):
    # use as global

    f, (ax1, ax2, ax3) = plt.subplots(1, 3, sharey=True, figsize=(9, 3))
    plt.subplots_adjust(wspace=0.05, hspace=0)
    plot_options(xvalues[0], options_1, ax1, xlabels[0], xlimit, ylimit)
    plot_options(xvalues[1], options_2, ax2, xlabels[1], xlimit, ylimit)
    plot_options(xvalues[2], options_3, ax3, xlabels[2], xlimit, ylimit)

    legend_col = 1

    ax1.legend(loc=2, ncol=legend_col)
    ax1.set_ylabel(ylabel)

    plt.savefig(output)
    print('output figure to ' + output)


records = (1, 2, 3, 4, 5)
none_records = [19.45, 37.74, 57.33, 68.32, 81.64]
sidefile_records = [20.87, 35.56, 49.83, 69.58, 89]
lock_records = [42.31, 85.1, 130.83, 174.81, 203.16]

updates = ('0%', '20%', '40%', '80%', '100%')
none_updates = [49.9, 52.58, 50.31, 51.51, 53.8]
sidefile_updates = [47.05, 47.95, 54.36, 51.65, 46.47]
lock_updates = [148.7, 133.65, 136.72, 114.19, 115.88]

sizes = (20, 100, 200, 500, 1000)
none_sizes = [54.54, 54.21, 55.53, 103.59, 204.95]
sidefile_sizes = [54.68, 58.03, 56.16, 119.14, 209.6]
lock_sizes = [124.05, 149.17, 147.93, 189.28, 269.14]

update_options = [ PlotOption(none_updates, 'Baseline', marker=markers[0], linestyle=antimatter_linestyle, color=antimatter_color),
                PlotOption(sidefile_updates, 'Side-file', marker=markers[1], linestyle=validation_linestyle, color=validation_color),
                PlotOption(lock_updates, 'Lock', marker=markers[2], linestyle=inplace_linestyle, color=validation_norepair_color)]
size_options = [ PlotOption(none_sizes, 'Baseline', marker=markers[0], linestyle=antimatter_linestyle, color=antimatter_color),
                PlotOption(sidefile_sizes, 'Side-file', marker=markers[1], linestyle=validation_linestyle, color=validation_color),
                PlotOption(lock_sizes, 'Lock', marker=markers[2], linestyle=inplace_linestyle, color=validation_norepair_color)]
record_options = [ PlotOption(none_records, 'Baseline', marker=markers[0], linestyle=antimatter_linestyle, color=antimatter_color),
                PlotOption(sidefile_records, 'Side-file', marker=markers[1], linestyle=validation_linestyle, color=validation_color),
                PlotOption(lock_records, 'Lock', marker=markers[2], linestyle=inplace_linestyle, color=validation_norepair_color)]

plot_shared_bitmap(
    [updates, records, sizes], update_options, record_options, size_options , result_base_path + 'bitmap.pdf',
                   ['Update Ratio', '#Records (Million)/Component', 'Record Size (Bytes)'], ylimit=280)

