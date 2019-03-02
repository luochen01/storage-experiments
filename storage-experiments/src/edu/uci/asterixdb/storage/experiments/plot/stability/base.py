
import numpy as np
import pandas
import matplotlib.pyplot as plt
import matplotlib
import os
import settings
from matplotlib.pyplot import legend
from datetime import datetime

base_path = '/Users/luochen/Documents/Research/experiments/results/flowcontrol/'
result_base_path = '/Users/luochen/Documents/Research/papers/lsm-stability-paper/'

zipf = "zipf"
uniform = "uniform"

uniform_path = base_path + uniform
zipf_path = base_path + zipf

if not os.path.exists(result_base_path):
    os.makedirs(result_base_path)

font_size = 12
font_weight = 100
label_size = 12
params = {
    'font.family': 'Times New Roman',
    'font.weight': font_weight,
    'axes.labelweight': font_weight,
    'figure.titleweight': font_weight,
    'axes.titlesize': font_size,
   'axes.labelsize': label_size,
   'legend.fontsize': font_size,
   'xtick.labelsize': label_size,
   'ytick.labelsize': label_size,
   'font.size': font_size,
   'lines.linewidth':1.5,
   'lines.markeredgewidth': 1,
   'lines.markersize':4,
   "legend.handletextpad":0.2,
   "legend.handlelength":1.5,
   'text.usetex': False,
   'savefig.bbox':'tight',
   'savefig.pad_inches':0,
   'figure.figsize':(3.75, 2.5),
   "legend.fancybox":True,
   "legend.shadow":False,
   "legend.framealpha":0,
   "legend.labelspacing":0.2,
   "legend.borderpad":0,
   "hatch.color":'white',
   "hatch.linewidth":'0.5',
    "xtick.direction": 'out',
    "ytick.direction": 'out',
}
markers = ['D', 's', 'o', '^', '*']
markers = ['o', '^', 'v', 'x', '*']

plt.rcParams.update(params)

time_xlabel = "Elapsed Time (s)"
write_ylabel = "Write Throughput (kops/s)"

point_ylabel = "Query Throughput (kops/s)"
short_ylabel = "Query Throughput (ops/s)"
long_ylabel = "Query Throughput (ops/min)"

latency_xlabel = "Percentile (%)"
latency_ylabel = "Latency (ms)"

component_ylabel = "Num of Disk Components"

component_gb_ylabel = "Total Disk Components(GB)"

write_window = 30

write_xlimit = 7200

latency_dists = np.array([90, 95, 99, 99.9, 99.99, 99.999])
latency_dist_labels = np.array(['90', '95', '99', '99.9', '99.99', '99.999'])

load_window = 2 * 60

component_window = 30

query_window = 30


def get_write_times(df, window=write_window):
    return np.arange(window, write_xlimit + 1, window)


def get_queries(df, window=query_window):
    data = df.iloc[:, 3]
    return data.groupby(np.arange(len(data)) // window).mean()


def get_write_rates(df, window=write_window):
    data = df.iloc[:, 1] / 1000
    data = data.loc[0:write_xlimit - 1]
    return data.groupby(np.arange(len(data)) // window).mean()


def get_latest_file(path, prefix):
    for f in os.listdir(path):
        if prefix in f and '.csv' not in f:
            s = f.replace(prefix, "")
            if s.startswith("-2019"):
                return path + f
    return None


def open_csv(path, sep='\t', header=1):
    try:
        csv = pandas.read_csv(path, sep=sep, header=header)
        return csv
    except:
        print('fail to parse ' + path)
        return None
    
    
class PlotOption(object):

    def __init__(self, x, y, legend='', color='black', linestyle='solid', marker=None, markevery=60, alpha=None, hatch=None, dashes=None):
        self.x = x
        self.y = y
        self.linestyle = linestyle
        self.marker = marker
        self.color = color
        self.legend = legend
        self.markevery = markevery
        self.alpha = alpha
        self.hatch = hatch
        self.dashes = dashes


def parse_latency_dists(latencies, count):
    thresholds = latency_dists * count / 100
    
    sum = 0
    i = 0
    prev_pair = [0, 0]
    percentiles = []
    for pair in latencies:
        sum += pair[1]
        if i < len(thresholds) and sum >= thresholds[i]:
            percentiles.append(prev_pair[0])
            i += 1
        prev_pair = pair
    if i < len(thresholds):
        percentiles.append(prev_pair[0])
        i += 1
    return percentiles


def plot_writes(options, output, ylimit=0, post=None):
    plot_basic(options, output, time_xlabel, write_ylabel, 1200, write_xlimit + 100, ylimit=ylimit, post=post)


def plot_queries(options, output, ylabel, ylimit=0, post=None):
    plot_basic(options, output, time_xlabel, ylabel, 1200, write_xlimit + 100, ylimit=ylimit, post=post)


def plot_latencies(options, output, ylimit=0, post=None):
    plot_basic(options, output, latency_xlabel, latency_ylabel, 1, xlimit=0, ylimit=ylimit, xtick_labels=latency_dist_labels, logy=True, post=post)


def plot_components(options, output, xstep=1200, ylimit=0, post=None):
    plot_basic(options, output, time_xlabel, component_ylabel, xstep=xstep, xlimit=write_xlimit + 100, ylimit=ylimit, post=post)


def plot_component_gbs(options, output, xstep=1200, ylimit=0, post=None):
    plot_basic(options, output, time_xlabel, component_gb_ylabel, xstep=xstep, xlimit=write_xlimit + 100, ylimit=ylimit, post=post)


def plot_basic(options, output, xlabel, ylabel, xstep, xlimit, ylimit, xtick_labels=[], logy=False, post=None):
    plt.figure(figsize=settings.fig_size)
    for option in options:
        if settings.plot_mode == 'plot':
            if option.dashes != None:
                plt.plot(option.x, option.y,
                     data=xtick_labels,
                      label=option.legend, color=option.color, linestyle=option.linestyle,
                      markerfacecolor=option.color, markeredgecolor=option.color, marker=option.marker, markevery=option.markevery,
                      dashes=option.dashes)
            else:
                plt.plot(option.x, option.y,
                     data=xtick_labels,
                      label=option.legend, color=option.color, linestyle=option.linestyle,
                      markerfacecolor=option.color, markeredgecolor=option.color, marker=option.marker, markevery=option.markevery)
        else:
            plt.scatter(option.x, option.y,
                        1,
                  label=option.legend, color=option.color, linestyle=option.linestyle)

    # plt.title(title)

    plt.xlabel(xlabel)
    plt.ylabel(ylabel)
    plt.ylim(0)
    if xlimit > 0:
        plt.xlim(0, xlimit)
    if len(xtick_labels) > 0:
        plt.xticks(np.arange(len(xtick_labels)), xtick_labels)
    else:
        plt.xticks(np.arange(0, xlimit, xstep))
    if ylimit > 0:
        plt.ylim(0, ylimit)
    if logy:
        plt.ylim(0.1)
        plt.yscale('log', basey=10)

    if len(options) > 1:
        plt.legend(loc=1, ncol=1)
    if post != None:
        post()
    plt.tight_layout()
    plt.savefig(output)
    plt.close()
    print('output figure to ' + output)


def parse_latencies(path, pattern):
    fp = open(path, 'r')

    a_list = []   
    count = 0 
    for line in fp:
        if pattern in line:
            parts = line.split(", ")
            try:
                a_list.append([float(parts[1]) / 1000, float(parts[2])])
                count += float(parts[2])
            except:
                print("ignore " + line)
    fp.close()
    return (a_list, count)


def parse_components(path):
    fp = open(path, 'r')
    
    times = []
    components = []

    time_str = "time:"
    component_str = "#components:"
    
    start_time = 0
    sum_components = 0
    previous_components = 0
    previous_time = 0
    for line in fp:
        if "#components:" in line:
            begin = line.index(time_str) + len(time_str)
            end = line.index(',', begin)
            time = float(line[begin:end]) / 1000
            begin = line.index(component_str) + len(component_str)
            component = int(line[begin: ])
            
            tmp = (min(start_time + component_window, time) - previous_time) * previous_components
            sum_components += tmp
            if time >= start_time + component_window:
                while time >= start_time + 2 * component_window:
                    times.append(start_time)
                    components.append(components[len(components) - 1])
                    start_time += component_window
                times.append(start_time)
                components.append(sum_components / component_window)
                sum_components = 0
                start_time += component_window
                sum_components += (time - start_time) * previous_components
            previous_time = time
            previous_components = component
    fp.close()
    components[0] = components[1]
    return (times, components)


def parse_component_sizes(path):
    component_str = "Component states "

    def parse_time(line):
         time_str = line[0:line.index(' ')]
         return datetime.strptime(time_str, '%H:%M:%S.%f')
    
    def parse_component_gbs(line):
        substr = line[line.index(component_str) + len(component_str): len(line)]
        parts = substr.split(", ")
        total_gb = 0
        for part in parts:
            if part.startswith('*'):
                part = part[1:-1]
            size = float(part.split(' ')[0])
            if "MB" in part:
                total_gb += size / 1024
            elif "GB" in part:
                total_gb += size
        return total_gb
    
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
            component_gb = parse_component_gbs(line)
            
            tmp = (min(start_time + component_window, time) - previous_time) * previous_components
            sum_components += tmp
            if time >= start_time + component_window:
                while time >= start_time + 2 * component_window:
                    times.append(start_time)
                    components.append(components[len(components) - 1])
                    start_time += component_window
                times.append(start_time)
                components.append(sum_components / component_window)
                sum_components = 0
                start_time += component_window
                sum_components += (time - start_time) * previous_components
            previous_time = time
            previous_components = component_gb
    fp.close()
    components[0] = components[1]
    return (times, components)


def get_greedy_scheduler(x, y):
    return PlotOption(x, y, linestyle='dashed', color='red', legend='greedy scheduler')


def get_fair_scheduler(x, y):
    return PlotOption(x, y, linestyle='solid', color='green', legend='fair scheduler')


def get_single_scheduler(x, y):
    return PlotOption(x, y, legend='single scheduler', color='orange', linestyle='-.')


def get_local_scheduler(x, y):
    return PlotOption(x, y, linestyle='dashed', color='blue', legend='fair scheduler (local)', dashes=(1, 1))


def get_option(x, y):
    return PlotOption(x, y)
