
import numpy as np
import pandas
import matplotlib as mt
import matplotlib.pyplot as plt
import os
from base import *
from pathlib import PurePath

query_base_path = base_path + 'query/'

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


def parse_experiment(prefix, pattern, skip=0):
    results = []
    for sel in sel_strs:
        file = prefix + "_" + sel + "_" + pattern + ".csv"
        print("processing file " + file)
        result = parse_csv(query_base_path + file, skip)
        results.append(result)
    return results


def plot_bar(xvalues, options, output, title, xlabel='Query Selectivity (%)', ylabel='Query Time (s)', ylimit=0):
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


sel_strs = ['0.00001', '0.00005', '0.0002', '0.001', '0.005', '0.02', '0.1']
sels = [0.001, 0.005, 0.02, 0.1, 0.5, 2, 10]

antimatter_1_prefix = "twitter_antimatter_UNIFORM_1"
antimatter_5_prefix = "twitter_antimatter_UNIFORM_5"
antimatter_skip = 2
antimatter_pattern = "false"
antimatter_1_results = parse_experiment(antimatter_1_prefix, antimatter_pattern, antimatter_skip)
# antimatter_5_results = parse_experiment(antimatter_5_prefix, antimatter_pattern, antimatter_skip)

validation_1_prefix = "twitter_validation_UNIFORM_1"
validation_5_prefix = "twitter_validation_UNIFORM_5"
validation_pattern = "true"
validation_skip = 2

validation_1_results = parse_experiment(validation_1_prefix, validation_pattern, validation_skip)
# validation_5_results = parse_experiment(validation_5_prefix, validation_pattern, validation_skip)

validation_norepair_1_prefix = "twitter_validation_norepair_UNIFORM_1"
validation_norepair_5_prefix = "twitter_validation_norepair_UNIFORM_5"
validation_norepair_pattern = "true"
validation_norepair_skip = 2

validation_norepair_1_results = parse_experiment(validation_norepair_1_prefix, validation_norepair_pattern, validation_norepair_skip)
# validation_norepair_5_results = parse_experiment(validation_norepair_5_prefix, validation_norepair_pattern, validation_norepair_skip)

plot_bar(sels, [ PlotOption(toTime(antimatter_1_results), 'eager', marker=markers[0], linestyle=antimatter_linestyle, color=antimatter_color),
                PlotOption(toTime(validation_1_results), 'validation', marker=markers[1], linestyle=validation_linestyle, color=validation_color),
                PlotOption(toTime(validation_norepair_1_results), 'validation (no repair)', marker=markers[2], linestyle=validation_norepair_linestyle, color=validation_norepair_color)],
                result_base_path + 'query-dataset-1-direct-validation.pdf', "Query Performance with Direct Validation Method")

