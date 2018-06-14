
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


def plot_bar(xvalues, options, output, title, xlabel='Time Range (Days)', ylabel='Query Time (s)', ylimit=0, legendloc=2, legendcol=1):
    # use as global
    plt.figure()
    x = np.arange(len(xvalues))
    numbars = float(len(options))
    i = 0
    barwidth = 0.2
    for option in options:
        plt.bar(x + (i - numbars / 2) * barwidth, option.data, align='edge', label=option.legend, color=option.color, width=barwidth)
        i += 1

    legend_col = 1
    plt.legend(loc=legendloc, ncol=legendcol)

    # plt.title(title)
    plt.xticks(x, xvalues)

    # plt.xlim(0, 310)
    if ylimit > 0:
        plt.ylim(0, ylimit)
    plt.xlabel(xlabel)
    plt.ylabel(ylabel)
    plt.savefig(output)
    print('output figure to ' + output)


records = [1, 2, 3, 4, 5]
none_records = [19.45, 37.74, 57.33, 68.32, 81.64]
sidefile_records = [20.87, 35.56, 49.83, 69.58, 89]
lock_records = [42.31, 85.1, 130.83, 174.81, 203.16]

updates = [0, 0.2, 0.4, 0.8, 1]
none_updates = [49.9, 52.58, 50.31, 51.51, 53.8]
sidefile_updates = [47.05, 47.95, 54.36, 51.65, 46.47]
lock_updates = [148.7, 133.65, 136.72, 114.19, 115.88]

sizes = [20, 100, 200, 500, 1000]
none_sizes = [54.54, 54.21, 55.53, 103.59, 204.95]
sidefile_sizes = [54.68, 58.03, 56.16, 119.14, 209.6]
lock_sizes = [124.05, 149.17, 147.93, 189.28, 269.14]

plot_bar(records, [ PlotOption(none_records, 'None', marker=markers[0], linestyle=antimatter_linestyle, color=antimatter_color),
                PlotOption(sidefile_records, 'Side-file', marker=markers[1], linestyle=validation_linestyle, color=validation_color),
                PlotOption(lock_records, 'Lock', marker=markers[2], linestyle=inplace_linestyle, color=validation_norepair_color)],
                result_base_path + 'bitmap-records.pdf', "", xlabel='#Records (Million)', legendloc=2)

plot_bar(updates, [ PlotOption(none_updates, 'None', marker=markers[0], linestyle=antimatter_linestyle, color=antimatter_color),
                PlotOption(sidefile_updates, 'Side-file', marker=markers[1], linestyle=validation_linestyle, color=validation_color),
                PlotOption(lock_updates, 'Lock', marker=markers[2], linestyle=inplace_linestyle, color=validation_norepair_color)],
                result_base_path + 'bitmap-updates.pdf', "", xlabel='Update Ratio', legendloc=1, ylimit=180, legendcol=1)

plot_bar(sizes, [ PlotOption(none_sizes, 'None', marker=markers[0], linestyle=antimatter_linestyle, color=antimatter_color),
                PlotOption(sidefile_sizes, 'Side-file', marker=markers[1], linestyle=validation_linestyle, color=validation_color),
                PlotOption(lock_sizes, 'Lock', marker=markers[2], linestyle=inplace_linestyle, color=validation_norepair_color)],
                result_base_path + 'bitmap-sizes.pdf', "", xlabel='Record Size (Bytes)', legendloc=2)

