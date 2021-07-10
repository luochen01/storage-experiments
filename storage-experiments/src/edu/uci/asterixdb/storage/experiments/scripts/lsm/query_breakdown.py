
import numpy as np
import pandas
import matplotlib as mt
import matplotlib.pyplot as plt
import os
import base
from base import *
from pathlib import PurePath

query_base_path = base_path + 'query-breakdown/'

time_index = 'time'
# sel_strs = ['0.00001', '0.000025', '0.00005', '0.0001', '0.00025' , '0.0005', '0.001', '0.01', '0.1', '0.2', '0.5', '0.999']
# sels = [0.001, 0.002, 0.005, 0.01, 0.025, 0.05, 0.1, 1, 10, 20, 50, 100]

sel_strs = ['0.00001', '0.000025', '0.00005', '0.0001', '0.00025', '0.001', '0.01', '0.1', '0.2', '0.5']
sels = [0.001, 0.002, 0.005, 0.01, 0.025, 0.1, 1, 10, 20, 50]


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


def parse_query_experiment(prefix, pattern, skips=1, values=sel_strs):
    results = []
    i = 0
    for sel in values:
        file = prefix + "_" + sel + "_" + pattern + ".csv"
        print("processing file " + file)
        result = parse_csv(query_base_path + file, skips)
        results.append(result)
        i += 1
    return results


prefix = "twitter_validation_norepair_UNIFORM_1"
seq_prefix = "twitter_validation_norepair_UNIFORM_SEQ"

baseline_pattern = "baseline"
batch_pattern = "batch"
batch_btree_pattern = "batch_btree"
batch_btree_bf_pattern = "batch_btree_bf"
batch_btree_bf_id_pattern = "batch_btree_bf_id"
scan_pattern = "scan"

baseline_results = parse_query_experiment(prefix, baseline_pattern)
batch_results = parse_query_experiment(prefix, batch_pattern)
batch_btree_results = parse_query_experiment(prefix, batch_btree_pattern)
batch_btree_bf_results = parse_query_experiment(prefix, batch_btree_bf_pattern)
batch_btree_bf_id_results = parse_query_experiment(prefix, batch_btree_bf_id_pattern)
scan_results = parse_query_experiment(prefix, scan_pattern)
seq_scan_results = parse_query_experiment(seq_prefix, scan_pattern)


def plot_bar(xvalues, options,
             line_options,
             output, title, xlabel='Query Selectivity (%)', ylabel='Query Time (s)', ylimit=0, legendloc=2, figsize=(3, 2.5), legend_alpha = 0):
    # use as global
    plt.figure(figsize=figsize)
    x = np.arange(len(xvalues))
    numbars = float(len(options))
    i = 0
    barwidth = 0.17
    for option in options:
        plt.bar(x + (i - numbars / 2) * barwidth, option.data, align='edge', label=option.legend, color=option.color, width=barwidth, alpha=option.alpha)
        i += 1

    plt.legend(loc=legendloc, ncol=1,framealpha=legend_alpha, fontsize=12)

    for option in line_options:
        plt.plot(x, option.data, label=option.legend, color=option.color, linestyle=option.linestyle,
                  markerfacecolor='none', markeredgecolor=option.color, marker=option.marker, markevery=1,
                  linewidth=1.0)

    # plt.title(title)
    plt.xticks(x, xvalues)
    plt.tick_params(axis='y', labelsize=12)

    # plt.xlim(0, 310)
    if ylimit > 0:
        plt.ylim(0, ylimit)
    plt.xlabel(xlabel)
    if ylabel != None:
        plt.ylabel(ylabel)
    plt.xlim([-0.5, len(x) - 0.5])
    if len(line_options) > 0:
#         box = dict(facecolor='white', alpha=0, linestyle='None', capstyle='round', edgecolor='none', joinstyle='round', pad=0.0)
#         plt.text(-0.05, 785, 'scan', bbox = box)
#         box = dict(facecolor='white', alpha=0.6, linestyle='None', capstyle='round', edgecolor='none', joinstyle='round', pad=0.0)
#         plt.text(-0.05, 460, 'scan (seq keys)', bbox=box)

        box = dict(facecolor='white', alpha=0.6, linestyle='None', capstyle='round', edgecolor='none', joinstyle='round', pad=0.0)
        plt.text(-0.05, 460, 'scan', bbox=box)

    plt.savefig(output)

    print('output figure to ' + output)


def plot_options(xvalues, options, ax, title, xlabel, ylimit=0, barwidth=0.17):
    x = np.arange(len(xvalues))
    numbars = float(len(options))
    i = 0
    for option in options:
        ax.bar(x + (i - numbars / 2) * barwidth, option.data, align='edge', label=option.legend, color=option.color, width=barwidth, alpha=option.alpha)
        i += 1
    ax.tick_params(axis='y', labelsize=12)
    ax.set_title(title)
    ax.set_xlabel(xlabel)
    ax.set_xticks(x)
    ax.set_xticklabels(xvalues)
    ax.set_xlim([-0.5, len(x) - 0.5])
    ax.set_ylim(0, ylimit)


def plot_shared_query(xvalues_1, xvalues_2, options_1, options_2, line_options, output, titles, xlabel='Query Selectivity (%)', ylabel='Query Time (s)', framealpha=0):
    # use as global
    f, (ax1, ax2) = plt.subplots(1, 2, sharey=False, figsize=(7, 2.2))
    plt.subplots_adjust(wspace=0.18, hspace=0)
    x = np.arange(len(xvalues_2))

    plot_options(xvalues_1, options_1, ax1, titles[0], xlabel, ylimit=250)
    plot_options(xvalues_2, options_2, ax2, titles[1], xlabel, ylimit=1999)

    hlength = 1.3
    fontsize = 12
    lspacing = 0.1
    ax1.legend(loc=2, ncol=1, framealpha=0, bbox_to_anchor=(-0.02, 1.03), handlelength=hlength, fontsize=fontsize, labelspacing=lspacing)
    ax1.set_ylabel(ylabel)

    ax2.legend(loc=2, ncol=1, framealpha=0.5, bbox_to_anchor=(-0.02, 1.03), handlelength=hlength, fontsize=fontsize, labelspacing=lspacing)

    for option in line_options:
        ax2.plot(x, option.data, label=option.legend, color=option.color, linestyle='dashed',
                  markerfacecolor='none', markeredgecolor=option.color, marker=None, markevery=None)

    box = dict(facecolor='white', alpha=0.5, linestyle='None', capstyle='round', edgecolor='none', joinstyle='round', pad=0.0)

    ax2.text(1, 785, 'full scan')
    ax2.text(1, 460, 'full scan (sequential keys)', bbox=box)

    plt.savefig(output)
    print('output figure to ' + output)


split_index = 5


def first_half(array):
    return array[:split_index]


def second_half(array):
    return array[split_index:]


color = 'blue'
alphas = [1, 0.8, 0.6, 0.4, 0.2]

options = []

low_options = [ PlotOption(toTime(first_half(baseline_results)), 'naive', marker=markers[0], linestyle=antimatter_linestyle, color=color, alpha=alphas[0]),
                PlotOption(toTime(first_half(batch_results)), 'batch', marker=markers[1], linestyle=validation_linestyle, color=color, alpha=alphas[1]),
                PlotOption(toTime(first_half(batch_btree_results)), 'batch/sLookup', marker=markers[2], linestyle=validation_norepair_linestyle, color=color, alpha=alphas[2]),
                PlotOption(toTime(first_half(batch_btree_bf_results)), 'batch/sLookup/bBF', marker=markers[3], linestyle=inplace_linestyle, color=color, alpha=alphas[3]),
                PlotOption(toTime(first_half(batch_btree_bf_id_results)), 'batch/sLookup/bBF/pID', marker=markers[4], linestyle=delete_btree_linestyle, color=color, alpha=alphas[4])]

high_options = [ PlotOption(toTime(second_half(baseline_results)), 'naive', marker=markers[0], linestyle=antimatter_linestyle, color=color, alpha=alphas[0]),
                PlotOption(toTime(second_half(batch_results)), 'batch', marker=markers[1], linestyle=validation_linestyle, color=color, alpha=alphas[1]),
                PlotOption(toTime(second_half(batch_btree_results)), 'batch/sLookup', marker=markers[2], linestyle=validation_norepair_linestyle, color=color, alpha=alphas[2]),
                PlotOption(toTime(second_half(batch_btree_bf_results)), 'batch/sLookup/bBF', marker=markers[3], linestyle=inplace_linestyle, color=color, alpha=alphas[3]),
                PlotOption(toTime(second_half(batch_btree_bf_id_results)), 'batch/sLookup/bBF/pID', marker=markers[4], linestyle=delete_btree_linestyle, color=color, alpha=alphas[4])]

plot_bar(first_half(sels), low_options,
                [],
                result_base_path + 'query-opt-breakdown-low.pdf', "", legendloc=2, figsize=(3.4,2.5))

# plot_bar(second_half(sels), high_options,
#                [ PlotOption(toTime(second_half(scan_results)), 'scan', marker=None, linestyle='dotted', color='red'),
#                 PlotOption(toTime(second_half(seq_scan_results)), 'scan (sequential keys)', marker=None, linestyle='dotted', color='green')],
#                 result_base_path + 'query-opt-breakdown-high.pdf', "", legendloc=2, figsize=(3.4,2.5), ylabel = None, legend_alpha = 0.5)
plot_bar(second_half(sels), high_options,
               [ PlotOption(toTime(second_half(seq_scan_results)), 'scan', marker=None, linestyle='dotted', color='red')],
                result_base_path + 'query-opt-breakdown-high.pdf', "", legendloc=2, figsize=(3.4,2.5), ylabel = None, legend_alpha = 0.5)



plot_shared_query(first_half(sels), second_half(sels), low_options, high_options,
               [ PlotOption(toTime(second_half(scan_results)), 'scan', marker=markers[0], linestyle='solid', color=antimatter_color),
                PlotOption(toTime(second_half(seq_scan_results)), 'scan (sequential keys)', marker=markers[1], linestyle='solid', color=validation_color)],
                result_base_path + 'query-opt-breakdown.pdf', ["Low Query Selectivity", 'High Query Selectivity'])

