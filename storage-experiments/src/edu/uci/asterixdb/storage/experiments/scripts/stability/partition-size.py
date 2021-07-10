
import numpy as np
import pandas
import matplotlib.pyplot as plt
from base import *
from pathlib import PurePath
from itertools import count

ylimit = 10

settings.init()

size_names = ['8MB', '64MB', '512MB', '4GB', '32GB']
sizes = [2048, 16384, 131072, 1048576, 8388608]

params = {
   'xtick.labelsize': 10
}
markers = ['D', 's', 'o', '^', '*']
markers = ['o', '^', 'v', 'x', '*']

plt.rcParams.update(params)


def get_round_scheduler(x, y):
    return PlotOption(x, y, legend='round-robin', color=green, marker = '^')


def get_choose_scheduler(x, y):
    return PlotOption(x, y, legend='choose-best', color='grey', linestyle='--', dashes=dashes, marker='s')


def parse_size_latencies(base_path, sizes, policy):
    size_latencies = []
    for size in sizes:
         (latencies, write_count) = parse_latencies(base_path + "write-partition-open-95-" + policy + str(size) + ".log", "[Intended-UPDATE]")
         latencies = parse_latency_dists(latencies, write_count)
         if len(latencies) < 5:
             size_latencies.append(0)
         else:
             size_latencies.append(latencies[3])
    return size_latencies


#settings.fig_size = [2.5, 2.5]

roundrobin_writes = np.array([4224, 4478, 4076, 4056, 4177]) / 1000
choosebest_writes = np.array([3972, 4352, 4014, 4056, 4182]) / 1000


def post():
        plt.legend(loc=2, ncol=1, bbox_to_anchor=None)


plot_basic(
    [
        get_round_scheduler(np.arange(len(size_ratios)), roundrobin_writes),
        get_choose_scheduler(np.arange(len(size_ratios)), choosebest_writes)
    ], result_base_path + 'write-partition-size.pdf', 'SSTable Size', write_ylabel, 1, xlimit=0, ylimit=15,
       xtick_labels=size_names, logy=False, post=post, title='(a) Testing Phase: Maximum\nWrite Throughput')

partition_base_path = base_path + "uniform/partition/"

roundrobin_latencies = parse_size_latencies(partition_base_path, sizes, "")
choosebest_latencies = parse_size_latencies(partition_base_path, sizes, "choosebest-")

#roundrobin_latencies = np.array([27391, 35967, 39023, 17524671, 1087373311]) / 1000/ 1000
#choosebest_latencies = np.array([66751, 34207, 39375, 15138495, 1017373311])/ 1000/1000


def post():
        plt.legend(loc=2, ncol=1, framealpha=0)
    

#settings.fig_size = [2.75, 2.5]

plot_basic(
    [
        get_round_scheduler(np.arange(len(size_ratios)), roundrobin_latencies),
        get_choose_scheduler(np.arange(len(size_ratios)), choosebest_latencies)
    ], result_base_path + 'write-partition-size-latency.pdf', 'SSTable Size', latency_ylabel, 1, xlimit=0, ylimit=2000, ymin=0.001,
       xtick_labels=size_names, logy=True, post=post, title='(b) Running Phase: 99%\nPercentile Write Latency')

