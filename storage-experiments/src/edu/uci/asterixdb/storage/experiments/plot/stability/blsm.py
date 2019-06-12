
import numpy as np
import pandas
import matplotlib.pyplot as plt
from base import *
from pathlib import PurePath
from itertools import count

ylimit = 40

seq_uniform = ','
seq_zipf = '\t'

settings.init()

settings.fig_size = (3.25, 2.5)


def get_uniform_option(x, y, marker=False):
    if marker == True:
        return PlotOption(x, y, color=green, legend='uniform', marker='s')
    else:
        return PlotOption(x, y, color=green, legend='uniform')


def get_zipf_option(x, y, marker=False):
    if marker == True:
        return PlotOption(x, y, color=red, legend='zipf', marker='^', linestyle = '--', dashes = dashes)
    else:
        return PlotOption(x, y, color=red, legend='zipf', linestyle='--', dashes = dashes)


uniform_base_path = base_path + 'uniform/blsm/'
df = open_csv(uniform_base_path + 'blsm-write.csv', sep=seq_uniform, header=None)
uniform_time = get_write_times(df)
uniform_data = get_write_rates(df)

zipf_base_path = base_path + 'zipf/blsm/'
df = open_csv(zipf_base_path + 'blsm-write.csv', sep=seq_zipf, header=None)
zipf_time = get_write_times(df)
zipf_data = get_write_rates(df)

plot_writes([get_zipf_option(zipf_time, zipf_data),
             get_uniform_option(uniform_time, uniform_data)], result_base_path + 'blsm-max.pdf', ylimit=50)

df = open_csv(uniform_base_path + 'blsm-open-95.csv', sep=seq_uniform, header=None)
uniform_time = get_write_times(df)
uniform_data = get_write_rates(df)

df = open_csv(zipf_base_path + 'blsm-open-95.csv', sep=seq_zipf, header=None)
zipf_time = get_write_times(df)
zipf_data = get_write_rates(df)


def post():
    leg = plt.legend(loc=1, ncol=1, bbox_to_anchor=None)


plot_writes([get_zipf_option(zipf_time, zipf_data),
             get_uniform_option(uniform_time, uniform_data)], result_base_path + 'blsm-open.pdf', ylimit=50, post=post)

(uniform_write_latencies, write_count) = parse_latencies(uniform_base_path + "blsm-open-95.log", "[UPDATE]")
uniform_write_latencies = parse_latency_dists(uniform_write_latencies, write_count)
    
(uniform_total_latencies, write_count) = parse_latencies(uniform_base_path + "blsm-open-95.log", "[Intended-UPDATE]")
uniform_total_latencies = parse_latency_dists(uniform_total_latencies, write_count)

(zipf_write_latencies, write_count) = parse_latencies(zipf_base_path + "blsm-open-95.log", "[UPDATE]")
zipf_write_latencies = parse_latency_dists(zipf_write_latencies, write_count)
    
(zipf_total_latencies, write_count) = parse_latencies(zipf_base_path + "blsm-open-95.log", "[Intended-UPDATE]")
zipf_total_latencies = parse_latency_dists(zipf_total_latencies, write_count)
    

def post():
    leg = plt.legend(loc=1, ncol=1, bbox_to_anchor=(1.02, 0.93), prop={'size': 11})
    
    vp = leg._legend_box._children[-1]._children[0] 
    #for c in vp._children: 
        #c._children.reverse() 
    vp.align="right" 

    
plot_latencies([
                PlotOption(np.arange(len(zipf_total_latencies)), zipf_total_latencies, marker='^', markevery=1, legend='zipf: write latency', color=red, linestyle='--', dashes = dashes),
                PlotOption(np.arange(len(zipf_write_latencies)), zipf_write_latencies, marker='s', markevery=1, legend='zipf: processing latency', color=red, linestyle='--', dashes = dashes),
                PlotOption(np.arange(len(uniform_total_latencies)), uniform_total_latencies, marker='^', markevery=1, legend='uniform: write latency', color=green),
                PlotOption(np.arange(len(uniform_write_latencies)), uniform_write_latencies, marker='s', markevery=1, legend='uniform: processing latency', color=green)],
                result_base_path + 'blsm-open-write-latency.pdf', ylimit=300, ymin=0.00005, post=post)

