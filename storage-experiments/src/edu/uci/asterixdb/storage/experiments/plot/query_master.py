
import numpy as np
import pandas
import matplotlib as mt
import matplotlib.pyplot as plt
import os
import base
from base import *
from pathlib import PurePath

query_base_path = '/Users/luochen/Desktop/master/'

time_index = 'time'
# sel_strs = ['0.00001', '0.000025', '0.00005', '0.0001', '0.00025' , '0.0005', '0.001', '0.01', '0.1', '0.2', '0.5', '0.999']
# sels = [0.001, 0.002, 0.005, 0.01, 0.025, 0.05, 0.1, 1, 10, 20, 50, 100]

sel_strs = ['0.00001', '0.000025', '0.00005', '0.0001', '0.00025', '0.0005', '0.001', '0.01', '0.1', '0.2', '0.5']
sels = [0.001, 0.002, 0.005, 0.01, 0.025, 0.05, 0.1, 1, 10, 20, 50]


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


index = 1

# 0: compressed
# 1: raw
scans = [180.775, 151.633 ]
suffixes = ["", "_raw"]

prefix = "twitter" + suffixes[index]
master_pattern = "master"
opt_pattern = "opt"

master_results = parse_query_experiment(prefix, master_pattern)
opt_results = parse_query_experiment(prefix, opt_pattern)


def plot_bar(xvalues, options,
             plot_line,
             output, title, xlabel='Query Selectivity (%)', ylabel='Query Time (s)', ylimit=0, legendloc=2):
    # use as global
    plt.figure(figsize=(6, 2.5))
    x = np.arange(len(xvalues))
    numbars = float(len(options))
    i = 0
    barwidth = 0.17
    for option in options:
        plt.bar(x + (i - numbars / 2) * barwidth, option.data, align='edge', label=option.legend, color=option.color, width=barwidth, alpha=option.alpha)
        i += 1

    plt.legend(loc=legendloc, ncol=2)

    # plt.title(title)
    plt.xticks(x, xvalues)

    # plt.xlim(0, 310)
    if ylimit > 0:
        plt.ylim(0, ylimit)
    plt.xlabel(xlabel)
    plt.ylabel(ylabel)
    plt.xlim([-0.5, len(x) - 0.5])
    
    if plot_line:
        plt.axhline(scans[index], 0, 1)
        plt.text(0.5, scans[index] + 20, 'scan')
    
    plt.savefig(output)

    print('output figure to ' + output)


split_index = 5


def first_half(array):
    return array[:split_index]


def second_half(array):
    return array[split_index:]


colors = ['red', 'green']

options = []

low_options = [ PlotOption(toTime(first_half(master_results)), 'master', marker=markers[0], linestyle=antimatter_linestyle, color=colors[0]),
                PlotOption(toTime(first_half(opt_results)), 'optimized', marker=markers[1], linestyle=validation_linestyle, color=colors[1])]

high_options = [ PlotOption(toTime(second_half(master_results)), 'master', marker=markers[0], linestyle=antimatter_linestyle, color=colors[0]),
                PlotOption(toTime(second_half(opt_results)), 'optimized', marker=markers[1], linestyle=validation_linestyle, color=colors[1])]

plot_bar(first_half(sels), low_options,
                False,
                '/Users/luochen/Desktop/query-opt-breakdown-low' + suffixes[index] + '.pdf', "", legendloc=2)

plot_bar(second_half(sels), high_options, True,
                '/Users/luochen/Desktop/query-opt-breakdown-high' + suffixes[index] + '.pdf', "", legendloc=2, ylimit=600)

