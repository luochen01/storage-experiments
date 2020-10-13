
import numpy as np
import pandas
import matplotlib.pyplot as plt
from base import *
from pathlib import PurePath
from itertools import count

ylimit = 40

xstep = 1800

settings.init()


#settings.fig_size = [2.75, 2.5]

def process(dist):
    prefix_base_path = base_path + dist + "/prefix/"
    print(prefix_base_path)
    df = open_csv(get_latest_file(prefix_base_path, 'write-prefix-max-10'), header=1)
    fair_time = get_write_times(df, load_window)
    fair_data = get_write_rates(df, load_window)
    
    #settings.fig_size = (3.75, 2.5)
    
    plot_writes([
        get_fair_scheduler(fair_time, fair_data)], result_base_path + 'write-prefix-' + dist + '.pdf', ylimit=ylimit)
    
    df = open_csv(get_latest_file(prefix_base_path, 'write-prefix-open-95-max-10-fast'), header=1)
    fair_time = get_write_times(df, write_window)
    fair_data = get_write_rates(df, write_window)
 
    df = open_csv(get_latest_file(prefix_base_path, 'write-prefix-open-95-max-10-greedy-fast'), header=1)
    greedy_time = get_write_times(df, write_window)
    greedy_data = get_write_rates(df, write_window)
    
    plot_writes([
        get_fair_scheduler(fair_time, fair_data),
        get_greedy_scheduler(greedy_time, greedy_data)], result_base_path + 'write-prefix-open-' + dist + '.pdf', ylimit=ylimit,
        xstep=xstep, title = '(a) Instantaneous Write\nThroughput')
    
    (fair_times, fair_components) = parse_components(prefix_base_path + "write-prefix-open-95-max-10-fast.log")
    (greedy_times, greedy_components) = parse_components(prefix_base_path + "write-prefix-open-95-max-10-greedy-fast.log")
    
    # settings.fig_size = (3.25, 2.5)
    
    def post():
        plt.legend(loc=4, ncol=1, bbox_to_anchor=None)
        #plt.plot([0, 7200], [50, 50], color='black', linestyle='dashed', linewidth='1')
    
    plot_components([
                    get_fair_scheduler(fair_times, fair_components),
                    get_greedy_scheduler(greedy_times, greedy_components)],
                    result_base_path + 'write-prefix-components-' + dist + '.pdf', ylimit=60, post=post, xstep=xstep, title = '(b) Number of Disk\nComponents')
  
    df = open_csv(get_latest_file(prefix_base_path, 'write-prefix-max-2'), header=1)
    fair_time = get_write_times(df, load_window)
    fair_data = get_write_rates(df, load_window)
    
    plot_writes([
        get_fair_scheduler(fair_time, fair_data)], result_base_path + 'write-prefix-slow-' + dist + '.pdf', ylimit=20)
    
    df = open_csv(get_latest_file(prefix_base_path, 'write-prefix-open-95-max-10-slow'), header=1)
    fair_time = get_write_times(df, write_window)
    fair_data = get_write_rates(df, write_window)
 
    df = open_csv(get_latest_file(prefix_base_path, 'write-prefix-open-95-max-10-greedy-slow'), header=1)
    greedy_time = get_write_times(df, write_window)
    greedy_data = get_write_rates(df, write_window)
    
    plot_writes([
        get_fair_scheduler(fair_time, fair_data),
        get_greedy_scheduler(greedy_time, greedy_data)], result_base_path + 'write-prefix-open-slow-' + dist + '.pdf',
        ylimit=18, xstep=xstep, ystep = 3, title = '(a) Instantaneous Write\nThroughput')
    
    (fair_times, fair_components) = parse_components(prefix_base_path + "write-prefix-open-95-max-10-slow.log")
    (greedy_times, greedy_components) = parse_components(prefix_base_path + "write-prefix-open-95-max-10-greedy-slow.log")
    
    # settings.fig_size = (3.25, 2.5)
    
    def post():
        plt.legend(loc=1, ncol=1, bbox_to_anchor=None)
    
    plot_components([
                    get_fair_scheduler(fair_times, fair_components),
                    get_greedy_scheduler(greedy_times, greedy_components)],
                    result_base_path + 'write-prefix-components-slow-' + dist + '.pdf', ylimit=30, xstep=xstep, title = '(b) Number of Disk\nComponents')

    (fair_times, fair_components) = parse_components(prefix_base_path + "write-prefix-open-95-max-2-slow.log")
    (greedy_times, greedy_components) = parse_components(prefix_base_path + "write-prefix-open-95-max-2-greedy-slow.log")
    
    plot_components([
                    get_fair_scheduler(fair_times, fair_components),
                    get_greedy_scheduler(greedy_times, greedy_components)],
                    result_base_path + 'write-prefix-components-slow-max-2-' + dist + '.pdf', ylimit=30, xstep=xstep)


process(uniform)
# process(zipf)

