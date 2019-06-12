
import numpy as np
import pandas
import matplotlib.pyplot as plt
from base import *
from pathlib import PurePath
from itertools import count

ylimit = 40

settings.init()


def parse_sizeratio_latencies(base_path, size_ratios, scheduler, policy):
    size_latencies = []
    for size in size_ratios:
         (latencies, write_count) = parse_latencies(base_path + "write-" + policy + "-open-95-" + str(size) + scheduler + ".log", "[Intended-UPDATE]")
         latencies = parse_latency_dists(latencies, write_count)
         if len(latencies) < 5:
             size_latencies.append(0)
         else:
             size_latencies.append(latencies[3])
    return size_latencies


settings.fig_size = (2.75, 2.5)

level_writes = np.array([8233, 6364, 5135, 4739, 3600]) / 0.95 / 1000
tier_writes = np.array([8250, 13764, 17604, 18815, 20112]) / 0.95 / 1000


def post():
        plt.legend(loc=2, ncol=1, bbox_to_anchor=None)
    

plot_basic(
    [
        PlotOption(np.arange(len(size_ratios)), level_writes, legend="leveling", color=green, marker='^', markevery=1),
        PlotOption(np.arange(len(size_ratios)), tier_writes, legend="tiering", color=green, marker='s', markevery=1),
    ], result_base_path + 'write-size-ratio.pdf', 'Size Ratio', write_ylabel, 1, xlimit=0, ylimit=30,
       xtick_labels=size_ratio_labels, logy=False, post=post)

level_base_path = base_path + "uniform/level/"

level_fair_latencies = parse_sizeratio_latencies(level_base_path, size_ratios, "", "level")
level_greedy_latencies = parse_sizeratio_latencies(level_base_path, size_ratios, "-greedy", "level")

tier_base_path = base_path + "uniform/tier/"

tier_fair_latencies = parse_sizeratio_latencies(tier_base_path, size_ratios, "", "tier")
tier_greedy_latencies = parse_sizeratio_latencies(tier_base_path, size_ratios, "-greedy", "tier")

def post():
        plt.legend(loc=2, ncol=1, framealpha = 0.5)
    

plot_basic(
    [
        PlotOption(np.arange(len(size_ratios)), level_fair_latencies, legend="fair + leveling", color=green, marker='^', markevery=1),
        PlotOption(np.arange(len(size_ratios)), level_greedy_latencies, legend="greedy + leveling", color=red, marker='^', markevery=1, linestyle = '--', dashes = dashes),
        PlotOption(np.arange(len(size_ratios)), tier_fair_latencies, legend="fair + tiering", color=green, marker='s', markevery=1),
        PlotOption(np.arange(len(size_ratios)), tier_greedy_latencies, legend="greedy + tiering", color=red, marker='s', markevery=1, linestyle = '--', dashes = dashes),
    ], result_base_path + 'write-size-ratio-latency.pdf', 'Size Ratio', latency_ylabel, 1, xlimit=0, ylimit=800, ymin=0.001,
       xtick_labels=size_ratio_labels, logy=True, post=post)

