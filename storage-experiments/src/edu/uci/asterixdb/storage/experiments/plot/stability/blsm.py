
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
def get_uniform_option(x, y):
    return PlotOption(x, y, color='green', legend='Uniform')


def get_zipf_option(x, y):
    return PlotOption(x, y, color='red', linestyle='dashed', legend='Zipf')


uniform_base_path = base_path + 'uniform/blsm/'
df = open_csv(uniform_base_path + 'blsm-write.csv', sep=seq_uniform, header=None)
uniform_time = get_write_times(df)
uniform_data = get_write_rates(df)

zipf_base_path = base_path + 'zipf/blsm/'
df = open_csv(zipf_base_path + 'blsm-write.csv', sep=seq_zipf, header=None)
zipf_time = get_write_times(df)
zipf_data = get_write_rates(df)

plot_writes([get_uniform_option(uniform_time, uniform_data),
             get_zipf_option(zipf_time, zipf_data)], result_base_path + 'blsm-max.pdf', ylimit=50)

df = open_csv(uniform_base_path + 'blsm-open-95.csv', sep=seq_uniform, header=None)
uniform_time = get_write_times(df)
uniform_data = get_write_rates(df)

df = open_csv(zipf_base_path + 'blsm-open-95.csv', sep=seq_zipf, header=None)
zipf_time = get_write_times(df)
zipf_data = get_write_rates(df)


def post():
    plt.legend(loc=2, ncol=1, bbox_to_anchor=None)


plot_writes([get_uniform_option(uniform_time, uniform_data),
             get_zipf_option(zipf_time, zipf_data)], result_base_path + 'blsm-open.pdf', ylimit=50, post=post)

(uniform_write_latencies, write_count) = parse_latencies(uniform_base_path + "blsm-open-95.log", "[UPDATE]")
uniform_write_latencies = parse_latency_dists(uniform_write_latencies, write_count)
    
(uniform_total_latencies, write_count) = parse_latencies(uniform_base_path + "blsm-open-95.log", "[Intended-UPDATE]")
uniform_total_latencies = parse_latency_dists(uniform_total_latencies, write_count)

(zipf_write_latencies, write_count) = parse_latencies(zipf_base_path + "blsm-open-95.log", "[UPDATE]")
zipf_write_latencies = parse_latency_dists(zipf_write_latencies, write_count)
    
(zipf_total_latencies, write_count) = parse_latencies(zipf_base_path + "blsm-open-95.log", "[Intended-UPDATE]")
zipf_total_latencies = parse_latency_dists(zipf_total_latencies, write_count)
    

def post():
    plt.legend(loc=1, ncol=1, bbox_to_anchor=(0.7, 0.8))

    
plot_latencies([PlotOption(np.arange(len(uniform_write_latencies)), uniform_write_latencies, marker='o', markevery=1, legend='Uniform-Processing', color='green'),
                PlotOption(np.arange(len(uniform_total_latencies)), uniform_total_latencies, marker='^', markevery=1, legend='Uniform-End-to-End', color='green'),
                PlotOption(np.arange(len(zipf_write_latencies)), zipf_write_latencies, marker='o', markevery=1, legend='Zipf-Processing', color='red', linestyle='dashed'),
                PlotOption(np.arange(len(zipf_total_latencies)), zipf_total_latencies, marker='^', markevery=1, legend='Zipf-End-to-End', color='red', linestyle='dashed')],
                result_base_path + 'blsm-open-write-latency.pdf', ylimit=500 * 1000, post=post)

