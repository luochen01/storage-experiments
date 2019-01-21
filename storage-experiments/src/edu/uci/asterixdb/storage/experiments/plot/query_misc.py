
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


def plot_bar(xvalues, options, output, title, xlabel='Batch Memory Size', ylabel='Query Time (s)', ylimit=0, legendloc=2):
    # use as global
    plt.figure(figsize=(2.7, 2.5))
    x = np.arange(len(xvalues))
    numbars = float(len(options))
    i = 0
    barwidth = 0.17
    for option in options:
        plt.bar(x + (i - numbars / 2) * barwidth, option.data, align='edge', label=option.legend, color=option.color, width=barwidth, alpha=option.alpha)
        i += 1

    legend_col = 1
    plt.legend(loc=legendloc, ncol=legend_col)

    # plt.title(title)
    plt.xticks(x, xvalues, fontsize=12)

    plt.xlim([-0.5, len(x) - 0.5])
    if ylimit > 0:
        plt.ylim(0, ylimit)
    plt.xlabel(xlabel)
    #plt.ylabel(ylabel)
    plt.savefig(output)
    print('output figure to ' + output)


batch_sizes = [0, 128, 1024, 4096, 16184]
batch_strs = ['0', '128KB', '1MB', '4MB', '16MB']
batch_skips = [2, 2, 2, 2, 2, 2, 2 ]
batch_prefix = "twitter_validation_norepair_UNIFORM_1"


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


# batch_0_001_results = parse_batch_experiment(batch_prefix, '0.00001', batch_skips)
batch_0_01_results = parse_batch_experiment(batch_prefix, '0.0001', batch_skips)
batch_0_1_results = parse_batch_experiment(batch_prefix, '0.001', batch_skips)
batch_1_results = parse_batch_experiment(batch_prefix, '0.01', batch_skips)
batch_10_results = parse_batch_experiment(batch_prefix, '0.1', batch_skips)



alphas = [1, 0.8, 0.6, 0.4, 0.2]
color = 'blue'
plot_bar(batch_strs, [
                PlotOption(toTime(batch_0_01_results), 'selectivity 0.01%', marker=markers[0], linestyle=validation_linestyle, color=color, alpha=alphas[0]),
                PlotOption(toTime(batch_0_1_results), 'selectivity 0.1%', marker=markers[1], linestyle=validation_norepair_linestyle, color=color, alpha=alphas[1]),
                PlotOption(toTime(batch_1_results), 'selectivity 1%', marker=markers[2], linestyle=inplace_linestyle, color=color, alpha=alphas[2]),
                PlotOption(toTime(batch_10_results), 'selectivity 10%', marker=markers[3], linestyle=inplace_linestyle, color=color, alpha=alphas[3])],
                result_base_path + 'query-batch-size.pdf', "Query Performance with Varying Batch Size", legendloc=1)
