
import numpy as np
import pandas
import matplotlib as mt
import matplotlib.pyplot as plt
import os
import base
from base import *
from pathlib import PurePath
from matplotlib.pyplot import legend
from bdb import bar

query_base_path = base_path + 'query/'



time_index = 'time'
# sel_strs = ['0.00001', '0.000025', '0.00005', '0.0001', '0.00025' , '0.0005', '0.001', '0.01']
# sels = [0.001, 0.002, 0.005, 0.01, 0.025, 0.05, 0.1, 1]

sel_strs = ['0.00001', '0.00005', '0.0001', '0.0005', '0.001', '0.01']
sels = [0.001, 0.005, 0.01, 0.05, 0.1, 1]

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


def diffTime(results1, results2):
    times = []
    for i in range(0, len(results1)):
        times.append(results1[i].time - results2[i].time)
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
        file = prefix + "_" + sel
        if pattern != None:
            file += "_" + pattern
        file += ".csv"
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

direct_validation_skips = [2, 2, 2, 2, 2, 2, 2]
pk_validation_skips = [100, 25, 10, 5, 2, 2]
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

sort_validation_norepair_1_prefix = "sort_twitter_validation_norepair_UNIFORM_1"

sort_sel_strs = ['0.00001', '0.0001', '0.001', '0.01', '0.1']
sort_sels = [0.001, 0.01, 0.1, 1, 10]

sort_results = parse_query_experiment(sort_validation_norepair_1_prefix, None, direct_validation_skips, values=sort_sel_strs)
sort_batch_results = parse_query_experiment(sort_validation_norepair_1_prefix, "batch", direct_validation_skips, values=sort_sel_strs)
batch_results = parse_query_experiment(validation_norepair_1_prefix, "16184", direct_validation_skips, values=sort_sel_strs)

print(matplotlib.font_manager.get_cachedir())


def plot_query(xvalues, options, output, xlabel='Query Selectivity (%)', ylabel='Query Time (s)', xlimit=110, framealpha=0.5, barwidth=0.3, legendsize=12, logy=False):
    # use as global
    plt.figure()
    x = np.arange(len(xvalues))
    numbars = float(len(options))
    i = 0
    for option in options:
        line = plt.bar(x + (i - numbars / 2) * barwidth, option.data, align='edge', label=option.legend, color=option.color, width=barwidth, alpha=option.alpha)
        i += 1
    # plt.set_title(title)
    plt.xlabel(xlabel)
    plt.xticks(x, xvalues)
    plt.xlim([-0.5, len(x) - 0.5])
    plt.legend(loc=2, ncol=1, framealpha=framealpha, fontsize=legendsize)
    if ylabel != None:
        plt.ylabel(ylabel)

    if logy:
        plt.yscale('log', basey=10)
        plt.ylim(0.05, 50)
        plt.yticks([0.1,1,10], ['0.1','1','10'])


    # ax1.set_ylim(0, 1000)
    # ax2.set_ylim(0, 1000)

    plt.savefig(output)
    print('output figure to ' + output)


def plot_options(xvalues, options, ax, title, xlabel, xlimit, ylimit, barwidth=0.18, xfontsize=None):
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
    f, (ax1, ax2) = plt.subplots(1, 2, sharey=True, figsize=(7, 2.2))
    plt.subplots_adjust(wspace=0.03, hspace=0)
    lines = plot_options(xvalues, options_1, ax1, titles[0], xlabel, xlimit, ylimit)
    plot_options(xvalues, options_2, ax2, titles[1], xlabel, xlimit, ylimit)
    # f.legend(handles=lines, loc='upper left', ncol=2, bbox_to_anchor=(0.065, 1.02), columnspacing=11.8)
    ax1.legend(framealpha=0, loc='upper left', bbox_to_anchor=(-0.025, 1.04), handlelength=1)
    ax1.set_ylabel(ylabel)
    ax1.set_yticklabels(['0','1','2','3'])
    
    box = dict(facecolor='white', alpha=0, linestyle='None', capstyle='round', edgecolor='none', joinstyle='round', pad=0.0)
    ax1.text(-0.5, 320, r'$\times$'+'100', bbox=box)
    ax2.legend(framealpha=0, loc='upper left', bbox_to_anchor=(-0.025, 1.04), handlelength=1)

    plt.savefig(output)
    print('output figure to ' + output)

color = 'blue'
alphas = [1, 0.9, 0.8, 0.7, 0.6, 0.5, 0.4, 0.3]
query_options = []
# query_options.append(
#      [ PlotOption(toTime(antimatter_1_results), 'eager', marker=markers[0], linestyle=antimatter_linestyle, color=antimatter_color),
#                 PlotOption(toTime(validation_norepair_1_direct_results), 'direct (no repair)', marker=markers[1], linestyle=validation_linestyle, color=validation_norepair_color, alpha=1),
#                 PlotOption(toTime(validation_norepair_1_pk_results), 'ts (no repair)', marker=markers[2], linestyle=validation_norepair_linestyle, color=validation_norepair_color, alpha=0.5),
#                 PlotOption(toTime(validation_1_direct_results), 'direct (repair)', marker=markers[1], linestyle=validation_linestyle, color=validation_color, alpha=1),
#                 PlotOption(toTime(validation_1_pk_results), 'ts (repair)', marker=markers[2], linestyle=validation_norepair_linestyle, color=validation_color, alpha=0.5)])
# 
# query_options.append([ PlotOption(toTime(antimatter_5_results), 'eager', marker=markers[0], linestyle=antimatter_linestyle, color=antimatter_color),
#                 PlotOption(toTime(validation_norepair_5_direct_results), 'direct (no repair)', marker=markers[1], linestyle=validation_linestyle, color=validation_norepair_color, alpha=1),
#                 PlotOption(toTime(validation_norepair_5_pk_results), 'ts (no repair)', marker=markers[2], linestyle=validation_norepair_linestyle, color=validation_norepair_color, alpha=0.5),
#                 PlotOption(toTime(validation_5_direct_results), 'direct (repair)', marker=markers[1], linestyle=validation_linestyle, color=validation_color, alpha=1),
#                 PlotOption(toTime(validation_5_pk_results), 'ts (repair)', marker=markers[2], linestyle=validation_norepair_linestyle, color=validation_color, alpha=0.5)])

query_options.append(
     [ PlotOption(toTime(antimatter_1_results), 'eager', marker=markers[0], linestyle=antimatter_linestyle, color=antimatter_color),
                PlotOption(toTime(validation_1_direct_results), 'direct validation', marker=markers[1], linestyle=validation_linestyle, color='blue', alpha=0.75)])

query_options.append([ PlotOption(toTime(antimatter_5_results), 'eager', marker=markers[0], linestyle=antimatter_linestyle, color=antimatter_color),
                PlotOption(toTime(validation_5_direct_results), 'direct validation', marker=markers[1], linestyle=validation_linestyle, color='blue', alpha=0.75)])


plot_shared_query(sels, query_options[0], query_options[1], result_base_path + "query-index.pdf", ['Update Ratio 0%', 'Update Ratio 50%'])

plot_query(sels, query_options[0],result_base_path + "query-index-0.pdf")
plot_query(sels, query_options[1],result_base_path + "query-index-50.pdf")

def plot_shared_index_only_query(xvalues, options_1, options_2, output, titles, xlabel='Query Selectivity (%)', ylabel='Query Time (s)', xlimit=110, ylimit=50):
    # use as global
    f, (ax1, ax2) = plt.subplots(1, 2, sharey=True, figsize=(6, 2.2))
    plt.subplots_adjust(wspace=0.03, hspace=0)
    barwidth = 0.5

    plot_options(xvalues, options_1, ax1, titles[0], xlabel, xlimit, ylimit, barwidth, xfontsize=12)
    plot_options(xvalues, options_2, ax2, titles[1], xlabel, xlimit, ylimit, barwidth, xfontsize=12)
    ax1.set_yscale('log', basey=10)
    ax2.set_yscale('log', basey=10)
    #ax1.set_yticklabels(['0.1','1','10'])
    ax1.set_ylim(0.05, ylimit)
    ax2.set_ylim(0.05, ylimit)
    
    ax1.legend(loc=2, ncol=1,bbox_to_anchor=(-0.025, 1.04),handlelength=1)
    #ax1.set_ylabel(ylabel, fontsize=12)
    ax1.set_yticks([0.1,1,10])
    ax1.set_yticklabels(['0.1','1','10'])

    ax2.legend(loc=2, ncol=1,bbox_to_anchor=(-0.025, 1.04),handlelength=1)

    plt.savefig(output)
    print('output figure to ' + output)


index_only_options = []

# index_only_options.append([ PlotOption(toTime(antimatter_1_indexonly_results), 'eager', marker=markers[0], linestyle=antimatter_linestyle, color=antimatter_color),
#                 PlotOption(toTime(validation_norepair_1_pk_indexonly_results), 'ts (no repair)', marker=markers[3], linestyle=inplace_linestyle, color=validation_norepair_color, alpha = 0.5),
#                 PlotOption(toTime(validation_1_pk_indexonly_results), 'ts (repair)', marker=markers[2], linestyle=validation_norepair_linestyle, color=validation_color, alpha = 0.5)])
# 
# index_only_options.append([ PlotOption(toTime(antimatter_5_indexonly_results), 'eager', marker=markers[0], linestyle=antimatter_linestyle, color=antimatter_color),
#                 PlotOption(toTime(validation_norepair_5_pk_indexonly_results), 'ts (no repair)', marker=markers[3], linestyle=inplace_linestyle, color=validation_norepair_color, alpha = 0.5),
#                 PlotOption(toTime(validation_5_pk_indexonly_results), 'ts (repair)', marker=markers[2], linestyle=validation_norepair_linestyle, color=validation_color, alpha = 0.5)])

index_only_options.append([ PlotOption(toTime(antimatter_1_indexonly_results), 'eager', marker=markers[0], linestyle=antimatter_linestyle, color=antimatter_color),
                PlotOption(toTime(validation_1_pk_indexonly_results), 'ts validation', marker=markers[2], linestyle=validation_norepair_linestyle, color='blue', alpha = 0.75)])

index_only_options.append([ PlotOption(toTime(antimatter_5_indexonly_results), 'eager', marker=markers[0], linestyle=antimatter_linestyle, color=antimatter_color),
                PlotOption(toTime(validation_5_pk_indexonly_results), 'ts validation', marker=markers[2], linestyle=validation_norepair_linestyle, color='blue', alpha = 0.75)])


plot_shared_index_only_query(indexonly_sels, index_only_options[0], index_only_options[1], result_base_path + "query-index-only.pdf", ['Update Ratio 0%', 'Update Ratio 50%'])
plot_query(sels, index_only_options[0],result_base_path + "query-index-only-0.pdf", logy=True)
plot_query(sels, index_only_options[1],result_base_path + "query-index-only-50.pdf", logy=True)



ts_cache_options = [PlotOption(toTime(validation_1_pk_results), 'ts validation', marker=markers[1], linestyle=validation_linestyle, color=validation_norepair_color, alpha=1),
                PlotOption(toTime(validation_1_pk_512M_results), 'ts validation (small cache)', marker=markers[2], linestyle=validation_linestyle, color=validation_norepair_color, alpha=0.5)]

plot_query(sels, ts_cache_options, result_base_path + "query-index-small-cache.pdf", "")

sort_options = [PlotOption(toTime(sort_results), 'No Batching', marker=markers[1], linestyle=validation_linestyle, color=validation_norepair_color, alpha=1),
                PlotOption(toTime(batch_results), 'Batching', marker=markers[2], linestyle=validation_linestyle, color=validation_norepair_color, alpha=0.5),
                PlotOption(diffTime(sort_batch_results, batch_results), 'Sorting', marker=markers[2], linestyle=validation_linestyle, color=validation_color, alpha=0.5)]

plt.rcParams.update({'figure.figsize':(3.25, 2.5)})


def plot_batch_sort(xvalues, nobatch, batch, sort, output, title, xlabel='Query Selectivity (%)', ylabel='Query Time (s)', xlimit=110, framealpha=0.5, barwidth=0.22, legendsize=14):
    # use as global
    plt.figure()
    x = np.arange(len(xvalues))
    plt.bar(x - barwidth, nobatch.data, align='edge', label=nobatch.legend, color=nobatch.color, width=barwidth, alpha=nobatch.alpha)
    plt.bar(x , batch.data, align='edge', label=batch.legend, color=batch.color, width=barwidth, alpha=batch.alpha)
    plt.bar(x , sort.data, align='edge', label=sort.legend, color=sort.color, width=barwidth, alpha=sort.alpha, bottom=batch.data)

    # plt.set_title(title)

    plt.xlabel(xlabel)
    plt.xticks(x, xvalues)
    plt.xlim([-0.5, len(x) - 0.5])
    plt.legend(loc=2, ncol=1, framealpha=framealpha, fontsize=legendsize)
    if ylabel != None:
        plt.ylabel(ylabel)

    # ax1.set_ylim(0, 1000)
    # ax2.set_ylim(0, 1000)

    plt.savefig(output)
    print('output figure to ' + output)


plot_batch_sort(sort_sels, sort_options[0], sort_options[1], sort_options[2], result_base_path + "query-batch-sort.pdf", "", framealpha=0, barwidth=0.3, legendsize=11)

