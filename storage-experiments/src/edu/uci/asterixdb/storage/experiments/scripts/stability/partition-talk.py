
import numpy as np
import pandas
import matplotlib.pyplot as plt
from base import *
from pathlib import PurePath
from itertools import count
from numpy.core.defchararray import partition

result_base_path = '/Users/luochen/Desktop/tmp/'

ylimit = 15

settings.init()

component_str = "Component state: "

dashes = (3, 1)

result_base_path = '/Users/luochen/Desktop/tmp/'

settings.init()

settings.fig_size = [3.5, 2.5]

params = {
   'lines.linewidth':1.0,
}

plt.rcParams.update(params)

font_size = 10

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


def get_round_scheduler(x, y, marker=False):
    if marker == True:
        return PlotOption(x, y, legend='round-robin', color='green', marker='s', markevery=1)
    else:
        return PlotOption(x, y, legend='round-robin', color='green')


def parse_components(line):
        substr = line[line.index(component_str) + len(component_str): len(line)]
        substr = substr.split(", ")[0]
        if substr.startswith('*'):
            substr = substr[1:-1]
        return int(substr.split(":")[1])


def parse_partitioned_components(path):

    def parse_time(line):
         time_str = line[0:line.index(' ')]
         return datetime.strptime(time_str, '%H:%M:%S.%f')
    
    fp = open(path, 'r')
    
    times = []
    components = []
    
    start_time = 0
    
    max_component = 0
    initial_time = None
    
    for line in fp:
        if "ConfigManager" in line and initial_time == None:
           initial_time = parse_time(line)
        if component_str in line:
            time = parse_time(line)
            time = (time - initial_time).total_seconds()
            if time < 0:
                time = 24 * 3600 + time
            
            component = parse_components(line)
            
            if time >= start_time + component_window:
                if max_component > 0:
                    times.append(start_time)
                    components.append(max_component)    
                while time >= start_time + component_window:
                    start_time += component_window
                max_component = 0
                
            if max_component < component:
                max_component = component
    fp.close()
    return (times, components)


def parse_partitioned_component_points(path):

    def parse_time(line):
         time_str = line[0:line.index(' ')]
         return datetime.strptime(time_str, '%H:%M:%S.%f')
    
    fp = open(path, 'r')
    
    times = []
    components = []
    
    start_time = 0
    sum_components = 0
    previous_components = 0
    previous_time = 0
    
    initial_time = None
    
    for line in fp:
        if "ConfigManager" in line and initial_time == None:
           initial_time = parse_time(line)
        if component_str in line:
            time = parse_time(line)
            time = (time - initial_time).total_seconds()
            if time < 0:
                time = 24 * 3600 + time
            component = parse_components(line)
            times.append(time)
            components.append(component)
    fp.close()
    return (times, components)


def process(dist):
    partition_base_path = base_path + dist + "/partition/"
    print(partition_base_path)

    df = open_csv(get_latest_file(partition_base_path, 'write-partition.csv'), header=1)
    round_time = get_write_times(df, load_window)
    round_data = get_write_rates(df, load_window)
    
    write_window = 10
    
    plot_writes([
        get_round_scheduler(round_time, round_data)],
        result_base_path + 'write-partition-' + dist + '.pdf', ylimit=ylimit, xstep=1800, ystep=3,
        title=None)
    
    df = open_csv(get_latest_file(partition_base_path, 'write-partition-open-95-fast.csv'), header=1)
    round_time = get_write_times(df, write_window)
    round_data = get_write_rates(df, write_window)
    
    plot_writes([
        get_round_scheduler(round_time, round_data)],
        result_base_path + 'write-partition-open-' + dist + '.pdf', ylimit=ylimit, xstep=1800, ystep=3,
        title=None)
    
    (round_latencies, write_count) = parse_latencies(partition_base_path + "write-partition-open-95-fast.log", "[Intended-UPDATE]")
    round_latencies = parse_latency_dists(round_latencies, write_count)
    
    plot_latencies([
                    get_round_scheduler(np.arange(len(round_latencies)), round_latencies, True)],
                    result_base_path + 'write-partition-write-latency-' + dist + '.pdf', ylimit=20, logy=False)
    
    df = open_csv(get_latest_file(partition_base_path, 'write-partition-fixed.csv'), header=1)
    round_time = get_write_times(df, load_window)
    round_data = get_write_rates(df, load_window)
    
    df = open_csv(get_latest_file(partition_base_path, 'write-partition-fixed-choosebest.csv'), header=1)
    choose_time = get_write_times(df, load_window)
    choose_data = get_write_rates(df, load_window)
    
    plot_writes([
        get_round_scheduler(round_time, round_data)],
        result_base_path + 'write-partition-fixed-' + dist + '.pdf', ylimit=ylimit, xstep=1800, ystep=3,
        title=None)
    
    df = open_csv(get_latest_file(partition_base_path, 'write-partition-open-95-slow.csv'), header=1)
    round_time = get_write_times(df, write_window)
    round_data = get_write_rates(df, write_window)
    
    df = open_csv(get_latest_file(partition_base_path, 'write-partition-open-95-slow-choosebest.csv'), header=1)
    choose_time = get_write_times(df, write_window)
    choose_data = get_write_rates(df, write_window)
 
    plot_writes([
        get_round_scheduler(round_time, round_data)],
        result_base_path + 'write-partition-open-fixed-' + dist + '.pdf', ylimit=ylimit, xstep=1800, ystep=3,
        title=None)
    
    (round_latencies, write_count) = parse_latencies(partition_base_path + "write-partition-open-95-slow.log", "[Intended-UPDATE]")
    round_latencies = parse_latency_dists(round_latencies, write_count)
    
    plot_latencies([
                    get_round_scheduler(np.arange(len(round_latencies)), round_latencies, True)],
                    result_base_path + 'write-partition-write-latency-fixed-' + dist + '.pdf', ylimit=1, logy=False)

    params = {
       'lines.linewidth':0.75,
    }
    
    plt.rcParams.update(params)
    
    (round_times, round_components) = parse_partitioned_components(partition_base_path + "write-partition-open-95-slow.log")
    # settings.plot_mode='scatter'
    plot_components([
                    get_round_scheduler(round_times, round_components)],
                    result_base_path + 'write-partition-fixed-component-' + dist + '.pdf', xstep=1800, ylimit=8)


process(uniform)
# process(zipf)

