
import numpy as np
import pandas
import matplotlib.pyplot as plt
from base import *
from pathlib import PurePath
from itertools import count

ylimit = 40

settings.init()


def process(dist):
    tier_base_path = base_path + dist + "/tier/"
    print(tier_base_path)
    df = open_csv(get_latest_file(tier_base_path, 'write-tier'), header=1)
    fair_time = get_write_times(df, load_window)
    fair_data = get_write_rates(df, load_window)
    
    df = open_csv(get_latest_file(tier_base_path, 'write-tier-greedy'), header=1)
    greedy_time = get_write_times(df, load_window)
    greedy_data = get_write_rates(df, load_window)
    
    df = open_csv(get_latest_file(tier_base_path, 'write-tier-single'), header=1)
    single_time = get_write_times(df, load_window)
    single_data = get_write_rates(df, load_window)
    
      
    df = open_csv(get_latest_file(tier_base_path, 'write-tier-strict'), header=1)
    local_time = get_write_times(df, load_window)
    local_data = get_write_rates(df, load_window)
    
    plot_writes([
        get_single_scheduler(single_time, single_data),
        get_fair_scheduler(fair_time, fair_data),
        get_local_scheduler(local_time, local_data),
        get_greedy_scheduler(greedy_time, greedy_data)
        ], result_base_path + 'write-tier-' + dist + '.pdf', ylimit=ylimit)
    
    df = open_csv(get_latest_file(tier_base_path, 'write-tier-open-95'), header=1)
    fair_time = get_write_times(df, write_window)
    fair_data = get_write_rates(df, write_window)
 
    df = open_csv(get_latest_file(tier_base_path, 'write-tier-open-95-greedy'), header=1)
    greedy_time = get_write_times(df, write_window)
    greedy_data = get_write_rates(df, write_window)

    df = open_csv(get_latest_file(tier_base_path, 'write-tier-open-95-single'), header=1)
    single_time = get_write_times(df, write_window)
    single_data = get_write_rates(df, write_window)
    
    plot_writes([
        get_single_scheduler(single_time, single_data),
        get_fair_scheduler(fair_time, fair_data),
        get_greedy_scheduler(greedy_time, greedy_data)], result_base_path + 'write-tier-open-' + dist + '.pdf', ylimit=ylimit)    
    
    (fair_latencies, write_count) = parse_latencies(tier_base_path + "write-tier-open-95.log", "[Intended-UPDATE]")
    fair_latencies = parse_latency_dists(fair_latencies, write_count)
      
    (greedy_latencies, write_count) = parse_latencies(tier_base_path + "write-tier-open-95-greedy.log", "[Intended-UPDATE]")
    greedy_latencies = parse_latency_dists(greedy_latencies, write_count)
    
    (single_latencies, write_count) = parse_latencies(tier_base_path + "write-tier-open-95-single.log", "[Intended-UPDATE]")
    single_latencies = parse_latency_dists(single_latencies, write_count)
    
    def post():
        plt.legend(loc=4, ncol=1, bbox_to_anchor=None)
    
    plot_latencies([
                    get_single_scheduler(np.arange(len(single_latencies)), single_latencies),
                    get_fair_scheduler(np.arange(len(fair_latencies)), fair_latencies),
                    get_greedy_scheduler(np.arange(len(greedy_latencies)), greedy_latencies)],
                    result_base_path + 'write-tier-write-latency-' + dist + '.pdf', ylimit=5000 * 1000,
                    post=post)
    
    (fair_times, fair_components) = parse_components(tier_base_path + "write-tier-open-95.log")
    (greedy_times, greedy_components) = parse_components(tier_base_path + "write-tier-open-95-greedy.log")
    
    settings.fig_size = (3.25, 2.5)
    
    plot_components([
                    get_fair_scheduler(fair_times, fair_components),
                    get_greedy_scheduler(greedy_times, greedy_components)],
                    result_base_path + 'write-tier-components-' + dist + '.pdf', ylimit=30,
                    post=post, xstep = 1800)

    (fair_times, fair_gbs) = parse_component_sizes(tier_base_path + "write-tier-open-95.log")
    (greedy_times, greedy_gbs) = parse_component_sizes(tier_base_path + "write-tier-open-95-greedy.log")
    plot_component_gbs([
                    get_fair_scheduler(fair_times, fair_gbs),
                    get_greedy_scheduler(greedy_times, greedy_gbs)],
                    result_base_path + 'write-tier-component-gb-' + dist + '.pdf',
                    post=post, xstep=1800)


process(uniform)
# process(zipf)

