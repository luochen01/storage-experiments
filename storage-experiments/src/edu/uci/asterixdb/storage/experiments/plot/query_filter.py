
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


def plot_bar(xvalues, options, output, title, xlabel='Time Range (Days)', ylabel='Query Time (s)', ylimit=0, legendloc=2):
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
    plt.legend(loc=legendloc, ncol=legend_col)

    # plt.title(title)
    plt.xticks(x, xvalues)

    # plt.xlim(0, 310)
    if ylimit > 0:
        plt.ylim(0, ylimit)
    plt.xlabel(xlabel)
    plt.ylabel(ylabel)
    plt.savefig(output)
    print('output figure to ' + output)


def plot_query(options, output, title, xlabel='Query', ylabel='Time (s)'):
    # use as global

    plt.figure()
    xvalues = []
    for option in options:
        xvalues = np.arange(len(option.data))
        plt.plot(xvalues, option.data, label=option.legend, color=option.color, linestyle=option.linestyle,
                  markerfacecolor='none', markeredgecolor=option.color, marker=option.marker, markevery=option.markevery,
                  linewidth=1.0)

    legend_col = 1
    plt.legend(loc=1, ncol=legend_col)
    plt.title(title)

    step = 60

    plt.xlabel(xlabel)
    # plt.xticks(np.arange(0, xlimit, step=step))
    # plt.xlim(0, xlimit)
    # plt.ylim(0, ylimit)
    plt.ylim(ymin=0)
    plt.ylabel(ylabel)
    plt.gca().yaxis.grid(linestyle='dotted')
    # plt.show()
    plt.savefig(output)
    print('output figure to ' + output)


filter_strs = ['1', '3', '7', '14', '30', '60', '180', '365']
filter_ranges = [1, 3, 7, 14, 30, 60, 180, 365]
filter_skips = [0] * len(filter_strs)
non_skips = np.empty(len(filter_strs))
non_skips = [0] * len(filter_strs)


def parse_filter_experiments(prefix, pattern, skips):
    results = []
    i = 0
    for filter in filter_ranges:
        file = prefix + "_" + pattern + "_" + str(filter) + ".csv"
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

inplace_1_recent_results = parse_filter_experiments(inplace_1_prefix, recent_pattern, filter_skips)
inplace_1_history_results = parse_filter_experiments(inplace_1_prefix, history_pattern, filter_skips)
inplace_1_dynamic_results = parse_filter_experiments(inplace_1_prefix, dynamic_pattern, non_skips)

inplace_5_recent_results = parse_filter_experiments(inplace_5_prefix, recent_pattern, filter_skips)
inplace_5_history_results = parse_filter_experiments(inplace_5_prefix, history_pattern, filter_skips)
inplace_5_dynamic_results = parse_filter_experiments(inplace_5_prefix, dynamic_pattern, non_skips)


plot_bar(filter_strs, [ PlotOption(toTime(antimatter_1_recent_results), 'eager method', marker=markers[0], linestyle=antimatter_linestyle, color=antimatter_color),
                PlotOption(toTime(validation_1_recent_results), 'validation method', marker=markers[1], linestyle=validation_linestyle, color=validation_color),
                PlotOption(toTime(inplace_1_recent_results), 'delete-bitmap method', marker=markers[2], linestyle=inplace_linestyle, color=inplace_color)],
                result_base_path + 'query-filter-recent-1.pdf', "Query Performance with 0% Update", legendloc=2)

plot_bar(filter_strs, [ PlotOption(toTime(antimatter_5_recent_results), 'eager method', marker=markers[0], linestyle=antimatter_linestyle, color=antimatter_color),
                PlotOption(toTime(validation_5_recent_results), 'validation method', marker=markers[1], linestyle=validation_linestyle, color=validation_color),
                PlotOption(toTime(inplace_5_recent_results), 'delete-bitmap method', marker=markers[2], linestyle=inplace_linestyle, color=inplace_color)],
                result_base_path + 'query-filter-recent-5.pdf', "Query Performance with 50% Update", legendloc=2)

plot_bar(filter_strs, [ PlotOption(toTime(antimatter_1_history_results), 'eager method', marker=markers[0], linestyle=antimatter_linestyle, color=antimatter_color),
                PlotOption(toTime(validation_1_history_results), 'validation method', marker=markers[1], linestyle=validation_linestyle, color=validation_color),
                PlotOption(toTime(inplace_1_history_results), 'delete-bitmap method', marker=markers[2], linestyle=inplace_linestyle, color=inplace_color)],
                result_base_path + 'query-filter-history-1.pdf', "Query Performance with 0% Update", legendloc=1, ylimit=700)

plot_bar(filter_strs, [ PlotOption(toTime(antimatter_5_history_results), 'eager method', marker=markers[0], linestyle=antimatter_linestyle, color=antimatter_color),
                PlotOption(toTime(validation_5_history_results), 'validation method', marker=markers[1], linestyle=validation_linestyle, color=validation_color),
                PlotOption(toTime(inplace_5_history_results), 'delete-bitmap method', marker=markers[2], linestyle=inplace_linestyle, color=inplace_color)],
                result_base_path + 'query-filter-history-5.pdf', "Query Performance with 50% Update", legendloc=1, ylimit=700)

plot_bar(filter_strs, [ PlotOption(toTime(antimatter_1_dynamic_results), 'eager method', marker=markers[0], linestyle=antimatter_linestyle, color=antimatter_color),
                PlotOption(toTime(validation_1_dynamic_results), 'validation method', marker=markers[1], linestyle=validation_linestyle, color=validation_color),
                PlotOption(toTime(inplace_1_dynamic_results), 'delete-bitmap method', marker=markers[2], linestyle=inplace_linestyle, color=inplace_color)],
                result_base_path + 'query-filter-dynamic-1.pdf', "Query Performance with 0% Update", legendloc=1, ylimit=500)

plot_bar(filter_strs, [ PlotOption(toTime(antimatter_5_dynamic_results), 'eager method', marker=markers[0], linestyle=antimatter_linestyle, color=antimatter_color),
                PlotOption(toTime(validation_5_dynamic_results), 'validation method', marker=markers[1], linestyle=validation_linestyle, color=validation_color),
                PlotOption(toTime(inplace_5_dynamic_results), 'delete-bitmap method', marker=markers[2], linestyle=inplace_linestyle, color=inplace_color)],
                result_base_path + 'query-filter-dynamic-5.pdf', "Query Performance with 50% Update", legendloc=1, ylimit=500)

