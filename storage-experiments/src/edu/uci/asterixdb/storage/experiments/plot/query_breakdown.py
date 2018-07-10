
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
sel_strs = ['0.00001', '0.000025', '0.00005', '0.0001', '0.00025' , '0.0005', '0.001', '0.01', '0.1', '0.2', '0.5', '0.999']
sels = [0.001, 0.002, 0.005, 0.01, 0.025, 0.05, 0.1, 1, 10, 20, 50, 100]


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
             output, title, xlabel='Query Selectivity (%)', ylabel='Query Time (s)', ylimit=0, legendloc=2):
    # use as global
    plt.figure(figsize=(6, 3))
    x = np.arange(len(xvalues))
    numbars = float(len(options))
    i = 0
    barwidth = 0.15
    for option in options:
        plt.bar(x + (i - numbars / 2) * barwidth, option.data, align='edge', label=option.legend, color=option.color, width=barwidth)
        i += 1

    legend_col = 2
    plt.legend(loc=legendloc, ncol=legend_col)

    for option in line_options:
        plt.plot(x, option.data, label=option.legend, color=option.color, linestyle=option.linestyle,
                  markerfacecolor='none', markeredgecolor=option.color, marker=option.marker, markevery=1,
                  linewidth=1.0)

    # plt.title(title)
    plt.xticks(x, xvalues)

    # plt.xlim(0, 310)
    if ylimit > 0:
        plt.ylim(0, ylimit)
    plt.xlabel(xlabel)
    plt.ylabel(ylabel)
    plt.xlim([-0.5, len(x) - 0.5])
    plt.text(3, 650, 'full scan')
    plt.text(1.8, 450, 'full scan (sequential keys)')

    plt.savefig(output)

    print('output figure to ' + output)


plot_bar(sels, [ PlotOption(toTime(baseline_results), 'naive', marker=markers[0], linestyle=antimatter_linestyle, color=antimatter_color),
                PlotOption(toTime(batch_results), 'batch', marker=markers[1], linestyle=validation_linestyle, color=inplace_color),
                PlotOption(toTime(batch_btree_results), 'batch+btree', marker=markers[2], linestyle=validation_norepair_linestyle, color=validation_color),
                PlotOption(toTime(batch_btree_bf_results), 'batch+btree+bf', marker=markers[3], linestyle=inplace_linestyle, color=validation_norepair_color),
                PlotOption(toTime(batch_btree_bf_id_results), 'batch+btree+bf+ID', marker=markers[4], linestyle=delete_btree_linestyle, color=delete_btree_color)],
               [ PlotOption(toTime(scan_results), 'scan', marker=None, linestyle='dotted', color='green'),
                PlotOption(toTime(seq_scan_results), 'scan (seq keys)', marker=None, linestyle='dotted', color='green')],
                result_base_path + 'query-opt-breakdown.pdf', "", legendloc=2)

