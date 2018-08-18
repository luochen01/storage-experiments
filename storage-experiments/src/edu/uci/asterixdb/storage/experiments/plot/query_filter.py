
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


class QueryResult(object):

    def __init__(self, csv):
        self.times = csv[time_index] / 1000
        self.time = np.mean(self.times)
        self.std = np.std(self.times)


def parse_csv(path, skip=0):
    try:
        csv = pandas.read_csv(path, sep='\t', header=0)
        return QueryResult(csv[skip:])
    except:
        print('fail to parse ' + path + sys.exc_info()[0])
        return None


def toTime(results):
    times = []
    for result in results:
        times.append(result.time)
    return times


def toStd(results):
    stds = []
    for result in results:
        stds.append(result.std)
    return stds


filter_strs = ['1', '3', '7', '14', '30', '60', '180', '365']
filter_ranges = [1, 3, 7, 14, 30, 60, 180, 365]
filter_skips = [0] * len(filter_strs)
non_skips = np.empty(len(filter_strs))
non_skips = [0] * len(filter_strs)


def parse_filter_experiments(prefix, pattern, skips, suffix=""):
    results = []
    i = 0
    for filter in filter_ranges:
        file = prefix + "_" + pattern + "_" + str(filter) + suffix + ".csv"
        print("processing file " + file)
        result = parse_csv(filter_base_path + file, skips[i])
        results.append(result)
        i += 1
    return results


antimatter_1_prefix = "twitter_antimatter_UNIFORM_1"
antimatter_5_prefix = "twitter_antimatter_UNIFORM_5"

recent_pattern = "RECENT"
history_pattern = "HISTORY"
dynamic_pattern = "dynamic"

antimatter_1_recent_results = parse_filter_experiments(antimatter_1_prefix, recent_pattern, filter_skips)
antimatter_1_history_results = parse_filter_experiments(antimatter_1_prefix, history_pattern, filter_skips)
antimatter_1_dynamic_results = parse_filter_experiments(antimatter_1_prefix, dynamic_pattern, non_skips)

antimatter_5_recent_results = parse_filter_experiments(antimatter_5_prefix, recent_pattern, filter_skips)
antimatter_5_history_results = parse_filter_experiments(antimatter_5_prefix, history_pattern, filter_skips)
antimatter_5_dynamic_results = parse_filter_experiments(antimatter_5_prefix, dynamic_pattern, non_skips)

validation_1_prefix = "twitter_validation_UNIFORM_1"
validation_5_prefix = "twitter_validation_UNIFORM_5"
validation_history_pattern = "HISTORY_PARTIAL"

validation_1_recent_results = parse_filter_experiments(validation_1_prefix, recent_pattern, filter_skips)
validation_1_history_results = parse_filter_experiments(validation_1_prefix, validation_history_pattern, filter_skips)
validation_1_dynamic_results = parse_filter_experiments(validation_1_prefix, dynamic_pattern, non_skips)

validation_5_recent_results = parse_filter_experiments(validation_5_prefix, recent_pattern, filter_skips)
validation_5_history_results = parse_filter_experiments(validation_5_prefix, validation_history_pattern, filter_skips)
validation_5_dynamic_results = parse_filter_experiments(validation_5_prefix, dynamic_pattern, non_skips)

inplace_1_prefix = "twitter_inplace_UNIFORM_1"
inplace_5_prefix = "twitter_inplace_UNIFORM_5"
inplace_4M_suffix = "_4M"

inplace_1_recent_4M_results = parse_filter_experiments(inplace_1_prefix, recent_pattern, filter_skips, inplace_4M_suffix)
inplace_1_history_4M_results = parse_filter_experiments(inplace_1_prefix, history_pattern, filter_skips, inplace_4M_suffix)
inplace_1_dynamic_4M_results = parse_filter_experiments(inplace_1_prefix, dynamic_pattern, non_skips)

inplace_5_recent_4M_results = parse_filter_experiments(inplace_5_prefix, recent_pattern, filter_skips, inplace_4M_suffix)
inplace_5_history_4M_results = parse_filter_experiments(inplace_5_prefix, history_pattern, filter_skips, inplace_4M_suffix)
inplace_5_dynamic_4M_results = parse_filter_experiments(inplace_5_prefix, dynamic_pattern, non_skips)

inplace_suffix = ""

inplace_1_recent_results = parse_filter_experiments(inplace_1_prefix, recent_pattern, filter_skips, inplace_suffix)
inplace_1_history_results = parse_filter_experiments(inplace_1_prefix, history_pattern, filter_skips, inplace_suffix)
inplace_1_dynamic_results = parse_filter_experiments(inplace_1_prefix, dynamic_pattern, non_skips)

inplace_5_recent_results = parse_filter_experiments(inplace_5_prefix, recent_pattern, filter_skips, inplace_suffix)
inplace_5_history_results = parse_filter_experiments(inplace_5_prefix, history_pattern, filter_skips, inplace_suffix)
inplace_5_dynamic_results = parse_filter_experiments(inplace_5_prefix, dynamic_pattern, non_skips)


def plot_options(xvalues, options, ax, title, xlabel, xlimit, ylimit=0, barwidth=0.22):
    x = np.arange(len(xvalues))
    numbars = float(len(options))
    i = 0
    for option in options:
        ax.bar(x + (i - numbars / 2) * barwidth, option.data, align='edge', label=option.legend, color=option.color, width=barwidth, alpha=option.alpha)
        i += 1
    ax.set_title(title)
    ax.set_xlabel(xlabel)
    ax.set_xticks(x)
    ax.set_xticklabels(xvalues)
    ax.set_xlim([-0.5, len(x) - 0.5])
    if ylimit > 0:
        ax.set_ylim(0, ylimit)


def plot_shared_query(xvalues, options_1, options_2, output, titles, xlabel='Time Range (Days)', ylabel='Query Time (s)', xlimit=110, framealpha=0):
    # use as global
    f, (ax1, ax2) = plt.subplots(1, 2, sharey=True, figsize=(6, 2.2))
    plt.subplots_adjust(wspace=0.03, hspace=0)
    plot_options(xvalues, options_1, ax1, titles[0], xlabel, xlimit)
    plot_options(xvalues, options_2, ax2, titles[1], xlabel, xlimit)

    ax1.legend(loc=2, ncol=1, framealpha=framealpha)
    ax1.set_ylabel(ylabel)

    #ax1.set_ylim(0, 1000)
    #ax2.set_ylim(0, 1000)

    ax1.set_yscale('log', basey=10)
    ax2.set_yscale('log', basey=10)

    ax1.set_ylim(ymin=1)
    ax2.set_ylim(ymin=1)

    plt.savefig(output)
    print('output figure to ' + output)



def plot_query(xvalues, options, output, title, xlabel='Time Range (Days)', ylabel='Query Time (s)', xlimit=110, framealpha=0):
    # use as global
    plt.figure()
    x = np.arange(len(xvalues))
    numbars = float(len(options))
    i = 0
    barwidth=0.22
    for option in options:
        plt.bar(x + (i - numbars / 2) * barwidth, option.data, align='edge', label=option.legend, color=option.color, width=barwidth, alpha=option.alpha)
        i += 1
    #plt.set_title(title)
    plt.xlabel(xlabel)
    plt.xticks(x, xvalues)
    plt.xlim([-0.5, len(x) - 0.5])
    plt.legend(loc=2, ncol=1, framealpha=framealpha)
    plt.ylabel(ylabel)

    #ax1.set_ylim(0, 1000)
    #ax2.set_ylim(0, 1000)

    plt.yscale('log', basey=10)

    plt.ylim(ymin=1)

    plt.savefig(output)
    print('output figure to ' + output)


recent_options = [
    [ PlotOption(toTime(antimatter_1_recent_results), 'eager', marker=markers[0], linestyle=antimatter_linestyle, color=antimatter_color),
                PlotOption(toTime(validation_1_recent_results), 'validation', marker=markers[1], linestyle=validation_linestyle, color=validation_color),
                PlotOption(toTime(inplace_1_recent_4M_results), 'delete-bitmap', marker=markers[2], linestyle=inplace_linestyle, color=inplace_color)],

    [ PlotOption(toTime(antimatter_5_recent_results), 'eager', marker=markers[0], linestyle=antimatter_linestyle, color=antimatter_color),
                PlotOption(toTime(validation_5_recent_results), 'validation', marker=markers[1], linestyle=validation_linestyle, color=validation_color),
                PlotOption(toTime(inplace_5_recent_4M_results), 'delete-bitmap', marker=markers[2], linestyle=inplace_linestyle, color=inplace_color)]
    ]

history_options = [
   [ PlotOption(toTime(antimatter_1_history_results), 'eager', marker=markers[0], linestyle=antimatter_linestyle, color=antimatter_color),
                PlotOption(toTime(validation_1_history_results), 'validation', marker=markers[1], linestyle=validation_linestyle, color=validation_color),
                PlotOption(toTime(inplace_1_history_4M_results), 'delete-bitmap', marker=markers[2], linestyle=inplace_linestyle, color=inplace_color)],

   [ PlotOption(toTime(antimatter_5_history_results), 'eager', marker=markers[0], linestyle=antimatter_linestyle, color=antimatter_color),
                PlotOption(toTime(validation_5_history_results), 'validation', marker=markers[1], linestyle=validation_linestyle, color=validation_color),
                PlotOption(toTime(inplace_5_history_4M_results), 'delete-bitmap', marker=markers[2], linestyle=inplace_linestyle, color=inplace_color)]]

#plot_shared_query(filter_strs, recent_options[0], recent_options[1], result_base_path + "query-filter-recent.pdf", ['Update Ratio 0%', 'Update Ratio 50%'])
#plot_shared_query(filter_strs, history_options[0], history_options[1], result_base_path + "query-filter-history.pdf", ['Update Ratio 0%', 'Update Ratio 50%'], framealpha=0.8)
plot_query(filter_strs, recent_options[1], result_base_path + "query-filter-recent-50.pdf", 'Update Ratio 50%')
plot_query(filter_strs, history_options[0], result_base_path + "query-filter-history-0.pdf", 'Update Ratio 0%', framealpha = 0.8)
plot_query(filter_strs, history_options[1], result_base_path + "query-filter-history-50.pdf", 'Update Ratio 50%', framealpha = 0.8)

