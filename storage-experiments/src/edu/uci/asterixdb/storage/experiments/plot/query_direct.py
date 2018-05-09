
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


def parse_experiment(prefix, pattern, skips):
    results = []
    i = 0
    for sel in sel_strs:
        file = prefix + "_" + sel + "_" + pattern + ".csv"
        print("processing file " + file)
        result = parse_csv(query_base_path + file, skips[i])
        results.append(result)
        i += 1
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


# sel_strs = ['0.00001', '0.00005', '0.0002', '0.001', '0.005', '0.02', '0.1']
# sels = [0.001, 0.005, 0.02, 0.1, 0.5, 2, 10]
sel_strs = ['0.00001', '0.000025', '0.00005', '0.0001', '0.00025' , '0.0005', '0.001', '0.01']
sels = [0.001, 0.002, 0.005, 0.01, 0.025, 0.05, 0.1, 1]
antimatter_skips = [2, 2, 2, 2, 2, 2, 2, 2]

antimatter_1_prefix = "twitter_antimatter_UNIFORM_1"
antimatter_5_prefix = "twitter_antimatter_UNIFORM_5"
antimatter_skip = 2
antimatter_pattern = "false"
antimatter_1_results = parse_experiment(antimatter_1_prefix, antimatter_pattern, antimatter_skips)
antimatter_5_results = parse_experiment(antimatter_5_prefix, antimatter_pattern, antimatter_skips)

validation_1_prefix = "twitter_validation_UNIFORM_1"
validation_5_prefix = "twitter_validation_UNIFORM_5"
validation_direct_pattern = "true"
validation_pk_pattern = "false"

direct_validation_skips = [2, 2, 2, 2, 2, 2, 2, 2]
pk_validation_skips = [100, 50, 25, 10, 5, 5, 2, 2]

validation_1_direct_results = parse_experiment(validation_1_prefix, validation_direct_pattern, direct_validation_skips)
validation_5_direct_results = parse_experiment(validation_5_prefix, validation_direct_pattern, direct_validation_skips)

validation_1_pk_results = parse_experiment(validation_1_prefix, validation_pk_pattern, pk_validation_skips)
validation_5_pk_results = parse_experiment(validation_5_prefix, validation_pk_pattern, pk_validation_skips)

validation_norepair_1_prefix = "twitter_validation_norepair_UNIFORM_1"
validation_norepair_5_prefix = "twitter_validation_norepair_UNIFORM_5"
validation_norepair_direct_pattern = "true"
validation_norepair_pk_pattern = "false"

validation_norepair_1_direct_results = parse_experiment(validation_norepair_1_prefix, validation_norepair_direct_pattern, direct_validation_skips)
validation_norepair_5_direct_results = parse_experiment(validation_norepair_5_prefix, validation_norepair_direct_pattern, direct_validation_skips)

validation_norepair_1_pk_results = parse_experiment(validation_norepair_1_prefix, validation_norepair_pk_pattern, pk_validation_skips)
validation_norepair_5_pk_results = parse_experiment(validation_norepair_5_prefix, validation_norepair_pk_pattern, pk_validation_skips)

plot_bar(sels, [ PlotOption(toTime(antimatter_1_results), 'eager', marker=markers[0], linestyle=antimatter_linestyle, color=antimatter_color),
                PlotOption(toTime(validation_1_direct_results), 'direct validation', marker=markers[1], linestyle=validation_linestyle, color=validation_color),
                PlotOption(toTime(validation_1_pk_results), 'pk index validation', marker=markers[2], linestyle=validation_norepair_linestyle, color=validation_norepair_color)],
                result_base_path + 'query-dataset-1-validation.pdf', "Query Performance with Validation Index")

plot_bar(sels, [ PlotOption(toTime(antimatter_5_results), 'eager', marker=markers[0], linestyle=antimatter_linestyle, color=antimatter_color),
                PlotOption(toTime(validation_5_direct_results), 'direct validation', marker=markers[1], linestyle=validation_linestyle, color=validation_color),
                PlotOption(toTime(validation_5_pk_results), 'pk index validation', marker=markers[2], linestyle=validation_norepair_linestyle, color=validation_norepair_color)],
                result_base_path + 'query-dataset-5-validation.pdf', "Query Performance with Validation Index")

plot_bar(sels, [ PlotOption(toTime(antimatter_1_results), 'eager', marker=markers[0], linestyle=antimatter_linestyle, color=antimatter_color),
                PlotOption(toTime(validation_norepair_1_direct_results), 'direct validation', marker=markers[1], linestyle=validation_linestyle, color=validation_color),
                PlotOption(toTime(validation_norepair_1_pk_results), 'pk index validation', marker=markers[2], linestyle=validation_norepair_linestyle, color=validation_norepair_color)],
                result_base_path + 'query-dataset-1-validation-norepair.pdf', "Query Performance with Validation Index")

plot_bar(sels, [ PlotOption(toTime(antimatter_5_results), 'eager', marker=markers[0], linestyle=antimatter_linestyle, color=antimatter_color),
                PlotOption(toTime(validation_norepair_5_direct_results), 'direct validation', marker=markers[1], linestyle=validation_linestyle, color=validation_color),
                PlotOption(toTime(validation_norepair_5_pk_results), 'pk index validation', marker=markers[2], linestyle=validation_norepair_linestyle, color=validation_norepair_color)],
                result_base_path + 'query-dataset-5-validation-norepair.pdf', "Query Performance with Validation Index")

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
    plt.legend(loc=2, ncol=legend_col)
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


plot_query([ PlotOption(validation_norepair_1_0_001_pk, 'update ratio 0%', linestyle=antimatter_linestyle, color=antimatter_color),
        PlotOption(validation_norepair_5_0_001_pk, 'update ratio 50%', linestyle=validation_norepair_linestyle, color=validation_norepair_color)],
        result_base_path + 'pk-validation-time-0.001%.pdf', "Selectivity 0.001%")


plot_query([ PlotOption(validation_norepair_1_0_005_pk, 'update ratio 0%', linestyle=antimatter_linestyle, color=antimatter_color),
        PlotOption(validation_norepair_5_0_005_pk, 'update ratio 50%', linestyle=validation_norepair_linestyle, color=validation_norepair_color)],
        result_base_path + 'pk-validation-time-0.005%.pdf', "Selectivity 0.005%")



plot_query([ PlotOption(validation_norepair_1_0_025_pk, 'update ratio 0%', linestyle=antimatter_linestyle, color=antimatter_color),
        PlotOption(validation_norepair_5_0_025_pk, 'update ratio 50%', linestyle=validation_norepair_linestyle, color=validation_norepair_color)],
        result_base_path + 'pk-validation-time-0.025%.pdf', "Selectivity 0.025%")


plot_query([ PlotOption(validation_norepair_1_0_1_pk, 'update ratio 0%', linestyle=antimatter_linestyle, color=antimatter_color),
        PlotOption(validation_norepair_5_0_1_pk, 'update ratio 50%', linestyle=validation_norepair_linestyle, color=validation_norepair_color)],
        result_base_path + 'pk-validation-time-0.1%.pdf', "Selectivity 0.1%")

