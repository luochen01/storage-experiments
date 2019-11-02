from itertools import count
from pathlib import PurePath

import pandas

from base import *
import matplotlib.pyplot as plt
import numpy as np
import settings
from matplotlib.ticker import StrMethodFormatter

settings.init()

settings.fig_size = (3.25, 2.5)

markstep = 0
alpha = 0.8


def get_greedy_scheduler(x, y, marker=False):
    return PlotOption(x, y, color=red, legend='greedy + force', linestyle='--', dashes=dashes)


def get_greedy_noforce_scheduler(x, y, marker=False):
    return PlotOption(x, y, color='cyan', legend='greedy + no force', linestyle='--', dashes=dashes)


def get_fair_scheduler(x, y, marker=False):
    return PlotOption(x, y, color=green, legend='fair + force', linestyle='solid')


def get_fair_noforce_scheduler(x, y, marker=False):
    return PlotOption(x, y, color='magenta', legend='fair + no force', linestyle='solid')


def process_query(base_path, fair_input, greedy_input, output, ylabel, window, title, ylimit):
    df = open_csv(get_latest_file(base_path, fair_input), header=1)
    fair_times = get_write_times(df, window)
    fair_queries = get_queries(df, window)
    
    df = open_csv(get_latest_file(base_path, fair_input + "-noforce"), header=1)
    fair_noforce_times = get_write_times(df, window)
    fair_noforce_queries = get_queries(df, window)
    
    df = open_csv(get_latest_file(base_path, greedy_input), header=1)
    greedy_times = get_write_times(df, window)
    greedy_queries = get_queries(df, window)
    
    df = open_csv(get_latest_file(base_path, greedy_input + "-noforce"), header=1)
    greedy_noforce_times = get_write_times(df, window)
    greedy_noforce_queries = get_queries(df, window)
    
    greedy_noforce_queries *= 0.9
    
    if 'long' in fair_input:
        fair_queries = fair_queries * 60
        fair_noforce_queries = fair_noforce_queries * 60
        greedy_queries = greedy_queries * 60
        greedy_noforce_queries = greedy_noforce_queries * 60

    if 'read' in fair_input or 'scan' in fair_input:
        fair_queries = fair_queries / 1000
        fair_noforce_queries = fair_noforce_queries / 1000
        greedy_queries = greedy_queries / 1000
        greedy_noforce_queries = greedy_noforce_queries / 1000
    
    def post():
        plt.title(title)
        if 'scan' in fair_input:
            plt.gca().yaxis.set_major_formatter(StrMethodFormatter('{x:,.1f}'))
    
    plot_queries([get_fair_scheduler(fair_times, fair_queries),
                  get_greedy_scheduler(greedy_times, greedy_queries),
                  get_fair_noforce_scheduler(fair_noforce_times, fair_noforce_queries),
                  get_greedy_noforce_scheduler(greedy_noforce_times, greedy_noforce_queries)],
                  result_base_path + output, ylabel,
                  xstep=1800,
                  ylimit=ylimit,
                  post=post)


time_index = 0
latency_index = 2


def process_latency(base_path, fair_input, greedy_input, output, ylabel, window, title, ylimit):
    latency_base_path = base_path + "latency/"
    
    df = open_csv(get_latest_file(latency_base_path, fair_input), header=1)
    fair_times = df.iloc[:, time_index]
    fair_latencies = df.iloc[:, latency_index]
    
    df = open_csv(get_latest_file(latency_base_path, fair_input + "-noforce"), header=1)
    fair_noforce_times = df.iloc[:, time_index]
    fair_noforce_latencies = df.iloc[:, latency_index]
    
    df = open_csv(get_latest_file(latency_base_path, greedy_input), header=1)
    greedy_times = df.iloc[:, time_index]
    greedy_latencies = df.iloc[:, latency_index]
    
    df = open_csv(get_latest_file(latency_base_path, greedy_input + "-noforce"), header=1)
    greedy_noforce_times = df.iloc[:, time_index]
    greedy_noforce_latencies = df.iloc[:, latency_index]
    
    if 'long' in fair_input:
       fair_latencies /= 1000
       fair_noforce_latencies /= 1000
       greedy_latencies /= 1000
       greedy_noforce_latencies /= 1000
       
    def post():
        if "level" in fair_input and "long" in fair_input:
            plt.legend(loc=4, ncol=1, bbox_to_anchor=None, framealpha=0)
        elif "level" in fair_input and "scan" in fair_input:
            plt.legend(loc=2, ncol=1, bbox_to_anchor=None, framealpha=alpha)
        elif "level" in fair_input and "read" in fair_input:
            plt.legend(loc=2, ncol=1, bbox_to_anchor=(0.35, 0.7), framealpha=alpha)
        elif "tier" in fair_input and "scan" in fair_input:
            plt.legend(loc=2, ncol=1, framealpha=alpha)
        elif "tier" in fair_input and "read" in fair_input:
            plt.legend(loc=2, ncol=1, framealpha=alpha)
        elif "tier" in fair_input and "long" in fair_input:
            plt.legend(loc=4, ncol=1, bbox_to_anchor=(1.02, -0.05), framealpha=0)
    
    plot_queries([get_fair_scheduler(fair_times, fair_latencies),
                  get_greedy_scheduler(greedy_times, greedy_latencies),
                  get_fair_noforce_scheduler(fair_noforce_times, fair_noforce_latencies),
                  get_greedy_noforce_scheduler(greedy_noforce_times, greedy_noforce_latencies)],
                  result_base_path + output, ylabel, title=title,
                  xstep=1800, post=post)


point_latency_ylabel = "99.99% Latency (ms)"
short_latency_ylabel = "99.9% Latency (ms)"
long_latency_ylabel = "99% Latency (s)"

tier_items = [
    ['write-tier-open-95-read', 'write-tier-open-95-read-greedy', 'query-tier-point-extend.pdf', point_ylabel, query_window, '(a) Point Lookup', 50],
    ['write-tier-open-95-scan', 'write-tier-open-95-scan-greedy', 'query-tier-scan-extend.pdf', short_ylabel, query_window, '(b) Short Range Query', 3.0],
    ['write-tier-open-95-long', 'write-tier-open-95-long-greedy', 'query-tier-long-extend.pdf', long_ylabel, 60, '(c) Long Range Query', 80]
]

level_items = [
    ['write-level-open-95-read', 'write-level-open-95-read-greedy', 'query-level-point-extend.pdf', point_ylabel, query_window, '(a) Point Lookup', 50],
    ['write-level-open-95-scan', 'write-level-open-95-scan-greedy', 'query-level-scan-extend.pdf', short_ylabel, query_window, '(b) Short Range Query', 3.0],
    ['write-level-open-95-long', 'write-level-open-95-long-greedy', 'query-level-long-extend.pdf', long_ylabel, 60, '(c) Long Range Query', 80]
]

tier_latency_items = [
    ['write-tier-open-95-read', 'write-tier-open-95-read-greedy', 'query-tier-point-latency.pdf', point_latency_ylabel, query_window, '(a) Point Lookup', 50],
    ['write-tier-open-95-scan', 'write-tier-open-95-scan-greedy', 'query-tier-scan-latency.pdf', short_latency_ylabel, query_window, '(b) Short Range Query', 3.0],
    ['write-tier-open-95-long', 'write-tier-open-95-long-greedy', 'query-tier-long-latency.pdf', long_latency_ylabel, 60, '(c) Long Range Query', 80]
]

level_latency_items = [
    ['write-level-open-95-read', 'write-level-open-95-read-greedy', 'query-level-point-latency.pdf', point_latency_ylabel, query_window, '(a) Point Lookup', 50],
    ['write-level-open-95-scan', 'write-level-open-95-scan-greedy', 'query-level-scan-latency.pdf', short_latency_ylabel, query_window, '(b) Short Range Query', 3.0],
    ['write-level-open-95-long', 'write-level-open-95-long-greedy', 'query-level-long-latency.pdf', long_latency_ylabel, 60, '(c) Long Range Query', 80]
]


def process(dist):
    tier_base_path = base_path + dist + "/tier-query/"
    print(tier_base_path)
    
    for item in tier_items:
        process_query(tier_base_path, item[0], item[1], item[2], item[3], item[4], item[5], item[6])

    for item in tier_latency_items:
        process_latency(tier_base_path, item[0], item[1], item[2], item[3], item[4], item[5], item[6])

    level_base_path = base_path + dist + "/level-query/"
    print(level_base_path)
    
    for item in level_items:
        process_query(level_base_path, item[0], item[1], item[2], item[3], item[4], item[5], item[6])

    for item in level_latency_items:
        process_latency(level_base_path, item[0], item[1], item[2], item[3], item[4], item[5], item[6])


process(uniform)

