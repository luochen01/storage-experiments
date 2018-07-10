
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
sel_strs = ['0.00001', '0.000025', '0.00005', '0.0001', '0.00025' , '0.0005', '0.001', '0.01']
sels = [0.001, 0.002, 0.005, 0.01, 0.025, 0.05, 0.1, 1]

indexonly_sel_strs = sel_strs[:]
# indexonly_sel_strs.append('0.1')
indexonly_sels = sels[:]
# indexonly_sels.append(10)


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
pk_validation_skips = [100, 50, 25, 10, 5, 5, 2, 2]
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


def plot_options(xvalues, options, ax, title, xlabel, xlimit, ylimit, barwidth=0.12, xfontsize=None):
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


def plot_shared_query(xvalues, options_1, options_2, output, titles, xlabel='Query Selectivity (%)', ylabel='Query Time (s)', xlimit=110, ylimit=350):
    # use as global
    set_large_fonts(shared_font_size)
    f, (ax1, ax2) = plt.subplots(1, 2, sharey=True, figsize=(15, 3))
    plt.subplots_adjust(wspace=0.03, hspace=0)
    lines = plot_options(xvalues, options_1, ax1, titles[0], xlabel, xlimit, ylimit)
    plot_options(xvalues, options_2, ax2, titles[1], xlabel, xlimit, ylimit)

    f.legend(handles=lines, loc='upper left', ncol=2, bbox_to_anchor=(0.06, 1), columnspacing=15)

    ax1.set_ylabel(ylabel)

    plt.savefig(output)
    print('output figure to ' + output)


query_options = []
query_options.append(
     [ PlotOption(toTime(antimatter_1_results), 'eager', marker=markers[0], linestyle=antimatter_linestyle, color=antimatter_color),
                PlotOption(toTime(validation_1_direct_results), 'direct validation', marker=markers[1], linestyle=validation_linestyle, color=validation_color, alpha=1),
                PlotOption(toTime(validation_1_pk_results), 'ts validation', marker=markers[2], linestyle=validation_norepair_linestyle, color=validation_color, alpha=0.7),
                PlotOption(toTime(validation_1_pk_512M_results), 'ts validation (small cache)', marker=markers[3], linestyle=inplace_linestyle, color=validation_color, alpha=0.4),
                PlotOption(toTime(validation_norepair_1_direct_results), 'direct validation (no repair)', marker=markers[1], linestyle=validation_linestyle, color=validation_norepair_color, alpha=1),
                PlotOption(toTime(validation_norepair_1_pk_results), 'ts validation (no repair)', marker=markers[2], linestyle=validation_norepair_linestyle, color=validation_norepair_color, alpha=0.7),
                PlotOption(toTime(validation_norepair_1_pk_512M_results), 'ts validation (no repair/small cache)', marker=markers[3], linestyle=inplace_linestyle, color=validation_norepair_color, alpha=0.4)])

query_options.append([ PlotOption(toTime(antimatter_5_results), 'eager', marker=markers[0], linestyle=antimatter_linestyle, color=antimatter_color),
                PlotOption(toTime(validation_5_direct_results), 'direct validation', marker=markers[1], linestyle=validation_linestyle, color=validation_color, alpha=1),
                PlotOption(toTime(validation_5_pk_results), 'ts validation', marker=markers[2], linestyle=validation_norepair_linestyle, color=validation_color, alpha=0.7),
                PlotOption(toTime(validation_5_pk_512M_results), 'ts validation (small cache)', marker=markers[3], linestyle=inplace_linestyle, color=validation_color, alpha=0.4),
                PlotOption(toTime(validation_norepair_5_direct_results), 'direct validation (no repair)', marker=markers[1], linestyle=validation_linestyle, color=validation_norepair_color, alpha=1),
                PlotOption(toTime(validation_norepair_5_pk_results), 'ts validation (no repair)', marker=markers[2], linestyle=validation_norepair_linestyle, color=validation_norepair_color, alpha=0.7),
                PlotOption(toTime(validation_norepair_5_pk_512M_results), 'ts validation (small cache)', marker=markers[3], linestyle=inplace_linestyle, color=validation_norepair_color, alpha=0.4), ])

plot_shared_query(sels, query_options[0], query_options[1], result_base_path + "query-index.pdf", ['Update Ratio 0%', 'Update Ratio 50%'])


def plot_shared_index_only_query(xvalues, options_1, options_2, output, titles, xlabel='Query Selectivity (%)', ylabel='Query Time (s)', xlimit=110, ylimit=22):
    # use as global
    set_large_fonts(shared_font_size)
    f, (ax1, ax2) = plt.subplots(1, 2, sharey=True, figsize=(8, 3))
    plt.subplots_adjust(wspace=0.03, hspace=0)
    barwidth = 0.2

    ax1.set_yscale('log', basey=10)
    ax2.set_yscale('log', basey=10)
    plot_options(xvalues, options_1, ax1, titles[0], xlabel, xlimit, ylimit, barwidth, xfontsize=12)
    plot_options(xvalues, options_2, ax2, titles[1], xlabel, xlimit, ylimit, barwidth, xfontsize=12)


    ax1.legend(loc=2, ncol=1)
    ax1.set_ylabel(ylabel)

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

# plot antimatter breakdown
antimatter_1_breakdown = pandas.read_csv(query_base_path + "twitter_antimatter_breakdown.csv", sep='\t', header=0)
secondary_time = antimatter_1_breakdown["secondary"] / 1000
primary_time = antimatter_1_breakdown["primary"] / 1000
plot_stack_bar(sels, [ PlotOption(secondary_time, 'secondary index search', marker=markers[0], linestyle=antimatter_linestyle, color=antimatter_color),
                PlotOption(primary_time, 'primary index lookup', marker=markers[1], linestyle=validation_linestyle, color=validation_color)],
                result_base_path + 'query-antimatter-breakdown.pdf', "Query Time Breakdown Analysis")

# plot convergence
validation_norepair_1_0_001_pk = pandas.read_csv(query_base_path + "twitter_validation_UNIFORM_1_0.00001_false.csv", sep='\t', header=0)[time_index] / 1000
validation_norepair_5_0_001_pk = pandas.read_csv(query_base_path + "twitter_validation_UNIFORM_5_0.00001_false.csv", sep='\t', header=0)[time_index] / 1000

validation_norepair_1_0_005_pk = pandas.read_csv(query_base_path + "twitter_validation_UNIFORM_1_0.00005_false.csv", sep='\t', header=0)[time_index] / 1000
validation_norepair_5_0_005_pk = pandas.read_csv(query_base_path + "twitter_validation_UNIFORM_5_0.00005_false.csv", sep='\t', header=0)[time_index] / 1000

validation_norepair_1_0_025_pk = pandas.read_csv(query_base_path + "twitter_validation_UNIFORM_1_0.00025_false.csv", sep='\t', header=0)[time_index] / 1000
validation_norepair_5_0_025_pk = pandas.read_csv(query_base_path + "twitter_validation_UNIFORM_5_0.00025_false.csv", sep='\t', header=0)[time_index] / 1000

validation_norepair_1_0_1_pk = pandas.read_csv(query_base_path + "twitter_validation_UNIFORM_1_0.001_false.csv", sep='\t', header=0)[time_index] / 1000
validation_norepair_5_0_1_pk = pandas.read_csv(query_base_path + "twitter_validation_UNIFORM_5_0.001_false.csv", sep='\t', header=0)[time_index] / 1000

# plot_query([ PlotOption(validation_norepair_1_0_001_pk, 'update ratio 0%', linestyle=antimatter_linestyle, color=antimatter_color),
#         PlotOption(validation_norepair_5_0_001_pk, 'update ratio 50%', linestyle=validation_norepair_linestyle, color=validation_norepair_color)],
#         result_base_path + 'pk-validation-time-0.001%.pdf', "Selectivity 0.001%")
#
# plot_query([ PlotOption(validation_norepair_1_0_005_pk, 'update ratio 0%', linestyle=antimatter_linestyle, color=antimatter_color),
#         PlotOption(validation_norepair_5_0_005_pk, 'update ratio 50%', linestyle=validation_norepair_linestyle, color=validation_norepair_color)],
#         result_base_path + 'pk-validation-time-0.005%.pdf', "Selectivity 0.005%")
#
# plot_query([ PlotOption(validation_norepair_1_0_025_pk, 'update ratio 0%', linestyle=antimatter_linestyle, color=antimatter_color),
#         PlotOption(validation_norepair_5_0_025_pk, 'update ratio 50%', linestyle=validation_norepair_linestyle, color=validation_norepair_color)],
#         result_base_path + 'pk-validation-time-0.025%.pdf', "Selectivity 0.025%")
#
# plot_query([ PlotOption(validation_norepair_1_0_1_pk, 'update ratio 0%', linestyle=antimatter_linestyle, color=antimatter_color),
#         PlotOption(validation_norepair_5_0_1_pk, 'update ratio 50%', linestyle=validation_norepair_linestyle, color=validation_norepair_color)],
#         result_base_path + 'pk-validation-time-0.1%.pdf', "Selectivity 0.1%")

