
import numpy as np
import pandas
import matplotlib.pyplot as plt
from base import *
from pathlib import PurePath
from itertools import count
import settings
from matplotlib.ticker import StrMethodFormatter

# ylimit = 15
font_size= 10

params = {
    'font.family': 'Calibri',
    'axes.titlesize': title_size,
   'axes.labelsize': font_size,
   'legend.fontsize': font_size,
   'xtick.labelsize': font_size,
   'ytick.labelsize': font_size,
   'font.size': font_size
}
plt.rcParams.update(params)

result_base_path = '/Users/luochen/Desktop/tmp/'

settings.init()

settings.fig_size = [3.5, 2.5]


def process(dist):
    level_base_path = base_path + dist + "/level/"
    print(level_base_path)

    df = open_csv(get_latest_file(level_base_path, 'write-level'), header=1)
    fair_time = get_write_times(df, load_window)
    fair_data = get_write_rates(df, load_window)
    
    df = open_csv(get_latest_file(level_base_path, 'write-level-greedy'), header=1)
    greedy_time = get_write_times(df, load_window)
    greedy_data = get_write_rates(df, load_window)
    
    df = open_csv(get_latest_file(level_base_path, 'write-level-single'), header=1)
    single_time = get_write_times(df, load_window)
    single_data = get_write_rates(df, load_window)
    
    df = open_csv(get_latest_file(level_base_path, 'write-level-strict'), header=1)
    local_time = get_write_times(df, load_window)
    local_data = get_write_rates(df, load_window)
  
    # write_window = 1
  
    plot_writes([
        get_single_scheduler(single_time, single_data),
        get_fair_scheduler(fair_time, fair_data),
        # get_local_scheduler(local_time, local_data),
        get_greedy_scheduler(greedy_time, greedy_data),
        ], result_base_path + 'write-level-' + dist + '.pdf', ylimit=18, ystep=3, title=None)
   
    df = open_csv(get_latest_file(level_base_path, 'write-level-open-95'), header=1)
    fair_time = get_write_times(df, write_window)
    fair_data = get_write_rates(df, write_window)
    
    df = open_csv(get_latest_file(level_base_path, 'write-level-open-95-greedy'), header=1)
    greedy_time = get_write_times(df, write_window)
    greedy_data = get_write_rates(df, write_window)
    
    df = open_csv(get_latest_file(level_base_path, 'write-level-open-95-single'), header=1)
    single_time = get_write_times(df, write_window)
    single_data = get_write_rates(df, write_window)
    
    df = open_csv(get_latest_file(level_base_path, 'write-level-open-95-strict'), header=1)
    local_time = get_write_times(df, write_window)
    local_data = get_write_rates(df, write_window)
    
    def post_write():
        plt.legend(loc=2, ncol=1, bbox_to_anchor=None, framealpha=0.8)
    
    settings.plot_mode = 'plot'
    
    plot_writes([
        get_single_scheduler(single_time, single_data),
        get_fair_scheduler(fair_time, fair_data),
        # get_local_scheduler(local_time, local_data),
        get_greedy_scheduler(greedy_time, greedy_data),
        ], result_base_path + 'write-level-open-' + dist + '.pdf', post=post_write, ylimit=20, title=None)
    
    (fair_latencies, write_count) = parse_latencies(level_base_path + "write-level-open-95.log", "[Intended-UPDATE]")
    fair_latencies = parse_latency_dists(fair_latencies, write_count)
      
    (greedy_latencies, write_count) = parse_latencies(level_base_path + "write-level-open-95-greedy.log", "[Intended-UPDATE]")
    greedy_latencies = parse_latency_dists(greedy_latencies, write_count)
    
    (single_latencies, write_count) = parse_latencies(level_base_path + "write-level-open-95-single.log", "[Intended-UPDATE]")
    single_latencies = parse_latency_dists(single_latencies, write_count)
    
    (local_fair_latencies, write_count) = parse_latencies(level_base_path + "write-level-open-95-strict.log", "[Intended-UPDATE]")
    local_fair_latencies = parse_latency_dists(local_fair_latencies, write_count)
    
    (local_greedy_latencies, write_count) = parse_latencies(level_base_path + "write-level-open-95-greedy-strict.log", "[Intended-UPDATE]")
    local_greedy_latencies = parse_latency_dists(local_greedy_latencies, write_count)
    
    def post():
        plt.legend(loc=4, ncol=1, bbox_to_anchor=(1.03, 0.4))
        # plt.legend(loc=4, ncol=1, bbox_to_anchor=None)
    
    plot_latencies([
                    get_single_scheduler(np.arange(len(single_latencies)), single_latencies, True),
                    get_fair_scheduler(np.arange(len(fair_latencies)), fair_latencies, True),
                    # get_local_scheduler(np.arange(len(local_latencies)), local_latencies),
                    get_greedy_scheduler(np.arange(len(greedy_latencies)), greedy_latencies, True)],
                    result_base_path + 'write-level-write-latency-' + dist + '.pdf', ylimit=4000, ymin=0.00005,
                    post=post, title=None)
    
    def post_latency():
        plt.legend(loc=2, ncol=1, framealpha=0.8)
    
    plot_latencies([
                    get_local_fair_scheduler(np.arange(len(fair_latencies)), local_fair_latencies, True),
                    get_local_greedy_scheduler(np.arange(len(fair_latencies)), local_greedy_latencies, True),
                    get_global_fair_scheduler(np.arange(len(fair_latencies)), fair_latencies, True),
                    get_global_greedy_scheduler(np.arange(len(fair_latencies)), greedy_latencies, True)],
                    result_base_path + 'write-level-write-latency-local-' + dist + '.pdf', ylimit=300,
                    ymin=-10, post=post_latency, logy=False, title=None)
    
    (single_times, single_components) = parse_components(level_base_path + "write-level-open-95-single.log")
    (fair_times, fair_components) = parse_components(level_base_path + "write-level-open-95.log")
    (greedy_times, greedy_components) = parse_components(level_base_path + "write-level-open-95-greedy.log")
      
    def post_components():
        plt.legend(loc=4, ncol=1, bbox_to_anchor=None)
        plt.gca().yaxis.set_major_formatter(StrMethodFormatter('{x:,.1f}'))

    plot_components([
                    get_single_scheduler(single_times, single_components),
                    get_fair_scheduler(fair_times, fair_components),
                    get_greedy_scheduler(greedy_times, greedy_components)],
                    result_base_path + 'write-level-components-' + dist + '.pdf', ylimit=8,
                    post=post_components, xstep=1800, title=None)
    
    (fair_times, fair_gbs) = parse_component_sizes(level_base_path + "write-level-open-95.log")
    (greedy_times, greedy_gbs) = parse_component_sizes(level_base_path + "write-level-open-95-greedy.log")
    plot_component_gbs([
                    get_fair_scheduler(fair_times, fair_gbs),
                    get_greedy_scheduler(greedy_times, greedy_gbs)],
                    result_base_path + 'write-level-component-gb-' + dist + '.pdf', ylimit=200,
                    post=post_components, xstep=1800)

    
process(uniform)
# process(zipf)

