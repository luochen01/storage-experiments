
import numpy as np
import pandas
import matplotlib as mt
import matplotlib.pyplot as plt
import os
from base import *
from pathlib import PurePath

index = ssd_index

ylimits = [[100, 100, 250], [80,80,80]]
query_base_path = base_path + devices[index] + '/query/'

time_index = 'time'

set_large_fonts(13)

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


def plot_shared_bitmap(xvalues, options_1, options_2, options_3, output, xlabels, ylabel='Merge Time (s)', xlimit=110, ylimit=[None, None, None]):
    # use as global

    f, (ax1, ax2, ax3) = plt.subplots(1, 3, sharey=False, figsize=(11, 2.5))
    plt.subplots_adjust(wspace=0.25, hspace=0)
    plot_options(xvalues[0], options_1, ax1, xlabels[0], xlimit, ylimit[0])
    plot_options(xvalues[1], options_2, ax2, xlabels[1], xlimit, ylimit[1])
    plot_options(xvalues[2], options_3, ax3, xlabels[2], xlimit, ylimit[2])

    legend_col = 1

    ax1.legend(loc=2, ncol=legend_col)
    ax2.legend(loc=2, ncol=legend_col)
    ax3.legend(loc=2, ncol=legend_col)

    ax1.set_ylabel(ylabel)
    ax2.set_ylabel(ylabel)
    ax3.set_ylabel(ylabel)

    plt.savefig(output)
    print('output figure to ' + output)


records = ('1M', '2M', '3M', '4M', '5M')
none_records = [[14.630, 19.863, 33.923, 45.327, 59.745 ], [7.31, 14.92, 22.50, 27.85, 36.894]]
sidefile_records = [[14.021, 20.146, 34.041, 49.752, 60.592 ], [8.096, 14.180, 21.060, 31.673, 37.809]]
lock_records = [[17.170, 32.831, 49.107, 65.636, 90.128  ], [13.586, 25.446, 36.620, 49.187, 62.514]]

updates = ('0%', '20%', '40%', '60', '80%', '100%')
none_updates = [[33.917, 37.276, 35.048, 32.306, 29.813, 34.823], [20.894, 22.442, 20.651, 21.404, 21.202, 21.314]]
sidefile_updates = [[35.557, 33.981, 33.523, 33.065, 35.531, 33.405], [22.192, 22.571, 21.243, 21.481, 22.534, 21.792]]
lock_updates = [[58.851, 52.617, 51.006, 51.799, 50.888, 54.375 ], [40.600, 38.877, 38.325, 39.668, 36.150, 38.302]]

sizes = (20, 100, 200, 500, 1000)
none_sizes = [[27.670, 30.893, 37.926, 109.271, 205.020  ], [17.942, 20.420, 23.058, 33.621, 57.175]]
sidefile_sizes = [[31.549, 38.391, 39.375, 110.654, 205.556], [19.539, 24.041, 24.586, 34.984, 58.399]]
lock_sizes = [[46.623, 51.682, 62.443, 123.736, 209.212 ], [35.463, 35.999 , 41.454, 47.959, 59.691]]

update_options = [ PlotOption(none_updates[index], 'Baseline', marker=markers[0], linestyle=antimatter_linestyle, color=antimatter_color),
                PlotOption(sidefile_updates[index], 'Side-file', marker=markers[1], linestyle=validation_linestyle, color=validation_color),
                PlotOption(lock_updates[index], 'Lock', marker=markers[2], linestyle=inplace_linestyle, color=validation_norepair_color)]
size_options = [ PlotOption(none_sizes[index], 'Baseline', marker=markers[0], linestyle=antimatter_linestyle, color=antimatter_color),
                PlotOption(sidefile_sizes[index], 'Side-file', marker=markers[1], linestyle=validation_linestyle, color=validation_color),
                PlotOption(lock_sizes[index], 'Lock', marker=markers[2], linestyle=inplace_linestyle, color=validation_norepair_color)]
record_options = [ PlotOption(none_records[index], 'Baseline', marker=markers[0], linestyle=antimatter_linestyle, color=antimatter_color),
                PlotOption(sidefile_records[index], 'Side-file', marker=markers[1], linestyle=validation_linestyle, color=validation_color),
                PlotOption(lock_records[index], 'Lock', marker=markers[2], linestyle=inplace_linestyle, color=validation_norepair_color)]

plot_shared_bitmap(
    [updates, records, sizes], update_options, record_options, size_options , result_base_path + devices[index] + '-bitmap.pdf',
                   ['Update Ratio', '#Records/Component', 'Record Size (Bytes)'], ylimit=ylimits[index])


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


# plot_bar(records, [ PlotOption(none_records[index], 'Baseline', marker=markers[0], linestyle=antimatter_linestyle, color=antimatter_color),
#                 PlotOption(sidefile_records[index], 'Side-file', marker=markers[1], linestyle=validation_linestyle, color=validation_color),
#                 PlotOption(lock_records[index], 'Lock', marker=markers[2], linestyle=inplace_linestyle, color=validation_norepair_color)],
#                 result_base_path + devices[index] + '-bitmap-records.pdf', "", xlabel='#Records (Million)', legendloc=2)
# 
# plot_bar(updates, [ PlotOption(none_updates[index], 'Baseline', marker=markers[0], linestyle=antimatter_linestyle, color=antimatter_color),
#                 PlotOption(sidefile_updates[index], 'Side-file', marker=markers[1], linestyle=validation_linestyle, color=validation_color),
#                 PlotOption(lock_updates[index], 'Lock', marker=markers[2], linestyle=inplace_linestyle, color=validation_norepair_color)],
#                 result_base_path + devices[index] + '-bitmap-updates.pdf', "", xlabel='Update Ratio', legendloc=1, ylimit=ylimits[index], legendcol=1)
# 
# plot_bar(sizes, [ PlotOption(none_sizes[index], 'Baseline', marker=markers[0], linestyle=antimatter_linestyle, color=antimatter_color),
#                 PlotOption(sidefile_sizes[index], 'Side-file', marker=markers[1], linestyle=validation_linestyle, color=validation_color),
#                 PlotOption(lock_sizes[index], 'Lock', marker=markers[2], linestyle=inplace_linestyle, color=validation_norepair_color)],
#                 result_base_path + devices[index] + '-bitmap-sizes.pdf', "", xlabel='Record Size (Bytes)', legendloc=2)

