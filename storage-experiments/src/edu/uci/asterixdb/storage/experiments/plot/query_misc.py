
import numpy as np
import pandas
import matplotlib as mt
import matplotlib.pyplot as plt
import os
from base import *
from pathlib import PurePath

query_base_path = base_path + 'query/'
misc_base_path = base_path + 'query-misc/'

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


def parse_experiment(prefix, pattern, skips, dir=query_base_path):
    results = []
    i = 0
    for sel in sel_strs:
        file = prefix + "_" + sel + "_" + pattern + ".csv"
        print("processing file " + file)
        result = parse_csv(dir + file, skips[i])
        results.append(result)
        i += 1
    return results


def plot_bar(xvalues, options, output, title, xlabel='Query Selectivity (%)', ylabel='Query Time (s)', ylimit=0, legendloc = 2):
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


batch_sizes = [0, 128, 256, 512, 1024, 4096, 16184]
batch_strs = ['0', '128KB', '256KB', '512KB', '1MB', '4MB', '16MB']
batch_skips = [2, 2, 2, 2, 2, 2, 2, 2 ]
batch_prefix = "twitter_antimatter_UNIFORM_1"


def parse_batch_experiment(prefix, pattern, skips):
    results = []
    i = 0
    for batch in batch_sizes:
        file = prefix + "_" + pattern + "_" + str(batch) + ".csv"
        print("processing file " + file)
        result = parse_csv(misc_base_path + file, skips[i])
        results.append(result)
        i += 1
    return results


batch_0_001_results = parse_batch_experiment(batch_prefix, '0.00001', batch_skips)
batch_0_01_results = parse_batch_experiment(batch_prefix, '0.0001', batch_skips)
batch_0_1_results = parse_batch_experiment(batch_prefix, '0.001', batch_skips)
batch_1_results = parse_batch_experiment(batch_prefix, '0.01', batch_skips)

plot_bar(batch_strs, [ PlotOption(toTime(batch_0_001_results), 'selectivity 0.001%', marker=markers[0], linestyle=antimatter_linestyle, color=antimatter_color),
                PlotOption(toTime(batch_0_01_results), 'selectivity 0.01%', marker=markers[1], linestyle=validation_linestyle, color=inplace_color),
                PlotOption(toTime(batch_0_1_results), 'selectivity 0.1%', marker=markers[2], linestyle=validation_norepair_linestyle, color=validation_color),
                PlotOption(toTime(batch_1_results), 'selectivity 1%', marker=markers[3], linestyle=inplace_linestyle, color=validation_norepair_color)],
                result_base_path + 'query-batch-size.pdf', "Query Performance with Varying Batch Size", legendloc=1)

sel_strs = ['0.00001', '0.000025', '0.00005', '0.0001', '0.00025' , '0.0005', '0.001', '0.01']
sels = [0.001, 0.002, 0.005, 0.01, 0.025, 0.05, 0.1, 1]
antimatter_skips = [2, 2, 2, 2, 2, 2, 2, 2]

antimatter_1_prefix = "twitter_antimatter_UNIFORM_1"
antimatter_5_prefix = "twitter_antimatter_UNIFORM_5"
antimatter_skip = 2
antimatter_pattern = "false"
antimatter_1_results = parse_experiment(antimatter_1_prefix, antimatter_pattern, antimatter_skips)
antimatter_5_results = parse_experiment(antimatter_5_prefix, antimatter_pattern, antimatter_skips)

antimatter_noid_pattern = "nocomponentid"

antimatter_1_noid_results = parse_experiment(antimatter_1_prefix, antimatter_noid_pattern, antimatter_skips, misc_base_path)
antimatter_5_noid_results = parse_experiment(antimatter_5_prefix, antimatter_noid_pattern, antimatter_skips, misc_base_path)

plot_bar(sels, [ PlotOption(toTime(antimatter_1_results), 'eager index w component id', marker=markers[0], linestyle=antimatter_linestyle, color=antimatter_color),
                PlotOption(toTime(antimatter_1_noid_results), 'eager index w/o component id', marker=markers[1], linestyle=validation_linestyle, color=validation_color)],
                result_base_path + 'query-cid-dataset-1.pdf', "Query Performance with Varying Batch Size")

plot_bar(sels, [ PlotOption(toTime(antimatter_5_results), 'eager index w component id', marker=markers[0], linestyle=antimatter_linestyle, color=antimatter_color),
                PlotOption(toTime(antimatter_5_noid_results), 'eager index w/o component id', marker=markers[1], linestyle=validation_linestyle, color=validation_color)],
                result_base_path + 'query-cid-dataset-5.pdf', "Query Performance with Varying Batch Size")
