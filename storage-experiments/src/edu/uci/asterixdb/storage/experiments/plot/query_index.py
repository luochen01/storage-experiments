
import numpy as np
import pandas
import matplotlib as mt
import matplotlib.pyplot as plt
import os
import base
from base import *
from pathlib import PurePath

query_base_path = base_path + 'query/'

time_index = 'time'
#sel_strs = ['0.00001', '0.000025', '0.00005', '0.0001', '0.00025' , '0.0005', '0.001', '0.01']
#sels = [0.001, 0.002, 0.005, 0.01, 0.025, 0.05, 0.1, 1]

sel_strs = ['0.00001', '0.00005', '0.0001', '0.00025' , '0.0005', '0.001', '0.01']
sels = [0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 1]

indexonly_sel_strs = ['0.00001', '0.00005', '0.0001', '0.0005', '0.001', '0.01']
indexonly_sels = [0.001, 0.005, 0.01, 0.05, 0.1, 1]

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


def parse_query_experiment(prefix, pattern, skips, values=sel_strs):
    results = []
    i = 0
    for sel in values:
        file = prefix + "_" + sel + "_" + pattern + ".csv"
        print("processing file " + file)
        result = parse_csv(query_base_path + file, skips[i])
        results.append(result)
        i += 1
    return results


def plot_stack_bar(xvalues, options, output, title, xlabel='Query Selectivity (%)', ylabel='Query Time (s)', ylimit=0):
    # use as global
    plt.figure()
    x = np.arange(len(xvalues))
    numbars = float(len(options))
    i = 0
    barwidth = 0.2
    for option in options:
        bottom_data = None
        if i > 0:
            bottom_data = options[i - 1].data
        plt.bar(x, option.data, align='edge', label=option.legend, color=option.color, width=barwidth, bottom=bottom_data)
        i += 1

    legend_col = 1
    plt.legend(loc=2, ncol=legend_col)

    # plt.title(title)
    plt.xticks(x, xvalues)

    # plt.xlim(0, 310)
    if ylimit > 0:
        plt.ylim(0, ylimit)
    plt.xlabel(xlabel)
    plt.ylabel(ylabel)
    plt.savefig(output)
    print('output figure to ' + output)


antimatter_skips = [2, 2, 2, 2, 2, 2, 2, 2]
antimatter_indexonly_skips = antimatter_skips[:]
antimatter_indexonly_skips.append(2)

antimatter_1_prefix = "twitter_antimatter_UNIFORM_1"
antimatter_5_prefix = "twitter_antimatter_UNIFORM_5"
antimatter_skip = 2
antimatter_pattern = "false"
antimatter_index_only_pattern = "indexonly"
antimatter_index_only_sort_pattern = "indexonly_sort"
antimatter_1_results = parse_query_experiment(antimatter_1_prefix, antimatter_pattern, antimatter_skips)
antimatter_5_results = parse_query_experiment(antimatter_5_prefix, antimatter_pattern, antimatter_skips)
antimatter_1_indexonly_results = parse_query_experiment(antimatter_1_prefix, antimatter_index_only_pattern, antimatter_indexonly_skips, indexonly_sel_strs)
antimatter_5_indexonly_results = parse_query_experiment(antimatter_5_prefix, antimatter_index_only_pattern, antimatter_indexonly_skips, indexonly_sel_strs)
antimatter_1_indexonly_sort_results = parse_query_experiment(antimatter_1_prefix, antimatter_index_only_sort_pattern, antimatter_indexonly_skips, indexonly_sel_strs)
antimatter_5_indexonly_sort_results = parse_query_experiment(antimatter_5_prefix, antimatter_index_only_sort_pattern, antimatter_indexonly_skips , indexonly_sel_strs)

validation_1_prefix = "twitter_validation_UNIFORM_1"
validation_5_prefix = "twitter_validation_UNIFORM_5"
validation_direct_pattern = "true"
validation_pk_pattern = "false"
validation_pk_512M_pattern = "false_512MB"
validation_pk_indexonly_pattern = "indexonly"

direct_validation_skips = [2, 2, 2, 2, 2, 2, 2, 2]
pk_validation_skips = [100, 25, 10, 5, 5, 2, 2]
pk_validation_indexonly_skips = pk_validation_skips[:]
pk_validation_indexonly_skips.append(2)

validation_1_direct_results = parse_query_experiment(validation_1_prefix, validation_direct_pattern, direct_validation_skips)
validation_5_direct_results = parse_query_experiment(validation_5_prefix, validation_direct_pattern, direct_validation_skips)

validation_1_pk_results = parse_query_experiment(validation_1_prefix, validation_pk_pattern, pk_validation_skips)
validation_1_pk_512M_results = parse_query_experiment(validation_1_prefix, validation_pk_512M_pattern, pk_validation_skips)
validation_5_pk_results = parse_query_experiment(validation_5_prefix, validation_pk_pattern, pk_validation_skips)
validation_5_pk_512M_results = parse_query_experiment(validation_5_prefix, validation_pk_512M_pattern, pk_validation_skips)

validation_1_pk_indexonly_results = parse_query_experiment(validation_1_prefix, validation_pk_indexonly_pattern, pk_validation_indexonly_skips, indexonly_sel_strs)
validation_5_pk_indexonly_results = parse_query_experiment(validation_5_prefix, validation_pk_indexonly_pattern, pk_validation_indexonly_skips, indexonly_sel_strs)

validation_norepair_1_prefix = "twitter_validation_norepair_UNIFORM_1"
validation_norepair_5_prefix = "twitter_validation_norepair_UNIFORM_5"
validation_norepair_direct_pattern = "true"
validation_norepair_pk_pattern = "false"
validation_norepair_pk_512M_pattern = "false_512MB"
validation_norepair_pk_indexonly_pattern = "indexonly"

validation_norepair_1_direct_results = parse_query_experiment(validation_norepair_1_prefix, validation_norepair_direct_pattern, direct_validation_skips)
validation_norepair_5_direct_results = parse_query_experiment(validation_norepair_5_prefix, validation_norepair_direct_pattern, direct_validation_skips)

validation_norepair_1_pk_results = parse_query_experiment(validation_norepair_1_prefix, validation_norepair_pk_pattern, pk_validation_skips)
validation_norepair_1_pk_512M_results = parse_query_experiment(validation_norepair_1_prefix, validation_norepair_pk_512M_pattern, pk_validation_skips)
validation_norepair_5_pk_results = parse_query_experiment(validation_norepair_5_prefix, validation_norepair_pk_pattern, pk_validation_skips)
validation_norepair_5_pk_512M_results = parse_query_experiment(validation_norepair_5_prefix, validation_norepair_pk_512M_pattern, pk_validation_skips)

validation_norepair_1_pk_indexonly_results = parse_query_experiment(validation_norepair_1_prefix, validation_norepair_pk_indexonly_pattern, pk_validation_indexonly_skips, indexonly_sel_strs)
validation_norepair_5_pk_indexonly_results = parse_query_experiment(validation_norepair_5_prefix, validation_norepair_pk_indexonly_pattern, pk_validation_indexonly_skips, indexonly_sel_strs)


def plot_options(xvalues, options, ax, title, xlabel, xlimit, ylimit, barwidth=0.16, xfontsize=None):
    x = np.arange(len(xvalues))
    numbars = float(len(options))
    i = 0
    lines = []
    for option in options:
        line = ax.bar(x + (i - numbars / 2) * barwidth, option.data, align='edge', label=option.legend, color=option.color, width=barwidth, alpha=option.alpha)
        i += 1
        lines.append(line)
    ax.set_title(title)
    ax.set_xlabel(xlabel)
    ax.set_xticks(x)
    if xfontsize != None:
        ax.set_xticklabels(xvalues, fontsize=xfontsize)
    else:
        ax.set_xticklabels(xvalues)
    ax.set_xlim([-0.5, len(x) - 0.5])
    ax.set_ylim(0, ylimit)
    return lines


def plot_shared_query(xvalues, options_1, options_2, output, titles, xlabel='Query Selectivity (%)', ylabel='Query Time (s)', xlimit=110, ylimit=310):
    # use as global
    f, (ax1, ax2) = plt.subplots(1, 2, sharey=False, figsize=(13, 2.2))
    plt.subplots_adjust(wspace=0.15, hspace=0)
    lines = plot_options(xvalues, options_1, ax1, titles[0], xlabel, xlimit, ylimit)
    plot_options(xvalues, options_2, ax2, titles[1], xlabel, xlimit, ylimit)
    #f.legend(handles=lines, loc='upper left', ncol=2, bbox_to_anchor=(0.065, 1.02), columnspacing=11.8)
    ax1.legend(framealpha=0.5)
    ax1.set_ylabel(ylabel)
    ax2.legend(framealpha=0.5)
    ax2.set_ylabel(ylabel)

    plt.savefig(output)
    print('output figure to ' + output)

color = 'blue'
alphas = [1, 0.9, 0.8, 0.7, 0.6, 0.5, 0.4, 0.3]
query_options = []
query_options.append(
     [ PlotOption(toTime(antimatter_1_results), 'eager', marker=markers[0], linestyle=antimatter_linestyle, color=antimatter_color),
                PlotOption(toTime(validation_1_direct_results), 'direct validation', marker=markers[1], linestyle=validation_linestyle, color=validation_color, alpha=1),
                PlotOption(toTime(validation_1_pk_results), 'ts validation', marker=markers[2], linestyle=validation_norepair_linestyle, color=validation_color, alpha=0.5),
                PlotOption(toTime(validation_norepair_1_direct_results), 'direct validation (no repair)', marker=markers[1], linestyle=validation_linestyle, color=validation_norepair_color, alpha=1),
                PlotOption(toTime(validation_norepair_1_pk_results), 'ts validation (no repair)', marker=markers[2], linestyle=validation_norepair_linestyle, color=validation_norepair_color, alpha=0.5)])

query_options.append([ PlotOption(toTime(antimatter_5_results), 'eager', marker=markers[0], linestyle=antimatter_linestyle, color=antimatter_color),
                PlotOption(toTime(validation_5_direct_results), 'direct validation', marker=markers[1], linestyle=validation_linestyle, color=validation_color, alpha=1),
                PlotOption(toTime(validation_5_pk_results), 'ts validation', marker=markers[2], linestyle=validation_norepair_linestyle, color=validation_color, alpha=0.5),
                PlotOption(toTime(validation_norepair_5_direct_results), 'direct validation (no repair)', marker=markers[1], linestyle=validation_linestyle, color=validation_norepair_color, alpha=1),
                PlotOption(toTime(validation_norepair_5_pk_results), 'ts validation (no repair)', marker=markers[2], linestyle=validation_norepair_linestyle, color=validation_norepair_color, alpha=0.5)])



plot_shared_query(sels, query_options[0], query_options[1], result_base_path + "query-index.pdf", ['Update Ratio 0%', 'Update Ratio 50%'])


def plot_shared_index_only_query(xvalues, options_1, options_2, output, titles, xlabel='Query Selectivity (%)', ylabel='Query Time (s)', xlimit=110, ylimit=50):
    # use as global
    f, (ax1, ax2) = plt.subplots(1, 2, sharey=False, figsize=(10, 2.2))
    plt.subplots_adjust(wspace=0.2, hspace=0)
    barwidth = 0.2

    plot_options(xvalues, options_1, ax1, titles[0], xlabel, xlimit, ylimit, barwidth)
    plot_options(xvalues, options_2, ax2, titles[1], xlabel, xlimit, ylimit, barwidth)
    ax1.set_yscale('log', basey=10)
    ax2.set_yscale('log', basey=10)

    ax1.set_ylim(0.05, ylimit)
    ax2.set_ylim(0.05, ylimit)

    ax1.legend(loc=2, ncol=1)
    ax1.set_ylabel(ylabel)

    ax2.legend(loc=2, ncol=1)
    ax2.set_ylabel(ylabel)


    plt.savefig(output)
    print('output figure to ' + output)


index_only_options = []

index_only_options.append([ PlotOption(toTime(antimatter_1_indexonly_results), 'eager', marker=markers[0], linestyle=antimatter_linestyle, color=antimatter_color),
                PlotOption(toTime(validation_1_pk_indexonly_results), 'ts validation', marker=markers[2], linestyle=validation_norepair_linestyle, color=validation_color),
                PlotOption(toTime(validation_norepair_1_pk_indexonly_results), 'ts validation (no repair)', marker=markers[3], linestyle=inplace_linestyle, color=validation_norepair_color)])

index_only_options.append([ PlotOption(toTime(antimatter_5_indexonly_results), 'eager', marker=markers[0], linestyle=antimatter_linestyle, color=antimatter_color),
                PlotOption(toTime(validation_5_pk_indexonly_results), 'ts validation', marker=markers[2], linestyle=validation_norepair_linestyle, color=validation_color),
                PlotOption(toTime(validation_norepair_5_pk_indexonly_results), 'ts validation (no repair)', marker=markers[3], linestyle=inplace_linestyle, color=validation_norepair_color)])

plot_shared_index_only_query(indexonly_sels, index_only_options[0], index_only_options[1], result_base_path + "query-index-only.pdf", ['Update Ratio 0%', 'Update Ratio 50%'])


def plot_query(xvalues, options, output, title, xlabel='Query Selectivity (%)', ylabel='Query Time (s)', xlimit=110, framealpha=0):
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
    plt.legend(loc=2, ncol=1, framealpha=0.5)
    plt.ylabel(ylabel)

    #ax1.set_ylim(0, 1000)
    #ax2.set_ylim(0, 1000)


    plt.savefig(output)
    print('output figure to ' + output)



ts_cache_options = [PlotOption(toTime(validation_1_pk_results), 'ts validation', marker=markers[1], linestyle=validation_linestyle, color=validation_norepair_color, alpha=1),
                PlotOption(toTime(validation_1_pk_512M_results), 'ts validation (small cache)', marker=markers[2], linestyle=validation_linestyle, color=validation_norepair_color, alpha=0.5)]


plot_query(sels, ts_cache_options, result_base_path + "query-index-small-cache.pdf", "")

