
import numpy as np
import pandas
import matplotlib.pyplot as plt
from base import *
from pathlib import PurePath
from itertools import count
import settings

# ylimit = 15

settings.init()


def get_open_scheduler(x, y, marker=False):
    if marker == True:
        return PlotOption(x, y, color=red, legend='No Write Limit', marker='s', linestyle = 'dashed')
    else:
        return PlotOption(x, y, color=red, legend='No Write Limit', linestyle = 'dashed')


def get_closed_scheduler(x, y, marker=False):
    if marker == True:
        return PlotOption(x, y, color=green, legend='With Write Limit', marker='^')
    else:
        return PlotOption(x, y, color=green, legend='With Write Limit')


def process(dist):
    level_base_path = base_path + dist + "/level/"
    print(level_base_path)

    df = open_csv(get_latest_file(level_base_path, 'write-level-open-95-burst'), header=1)
    open_time = get_write_times(df, write_window)
    open_data = get_write_rates(df, write_window)
    
    df = open_csv(get_latest_file(level_base_path, 'write-level-open-95-burst-closed'), header=1)
    closed_time = get_write_times(df, write_window)
    closed_data = get_write_rates(df, write_window)
    
    settings.fig_size = (3, 2.5)
    
    def post_write():
        plt.legend(loc=2, ncol=1, bbox_to_anchor=None, framealpha=0)
    
    settings.plot_mode = 'plot'
    
    plot_writes([
        get_closed_scheduler(closed_time, closed_data),
        get_open_scheduler(open_time, open_data),
        ], result_base_path + 'write-level-open-bust-' + dist + '.pdf', post=post_write, xstep=1800, ylimit=20)
    
    (open_latencies, write_count) = parse_latencies(level_base_path + "write-level-open-95-burst.log", "[Intended-UPDATE]")
    open_latencies = parse_latency_dists(open_latencies, write_count)
      
    (closed_latencies, write_count) = parse_latencies(level_base_path + "write-level-open-95-burst-closed.log", "[Intended-UPDATE]")
    closed_latencies = parse_latency_dists(closed_latencies, write_count)
    
    def post():
        plt.legend(loc=4, ncol=1, bbox_to_anchor=(1, -0.05))
        # plt.legend(loc=4, ncol=1, bbox_to_anchor=None)
    
    plot_latencies([
                    get_closed_scheduler(np.arange(len(closed_latencies)), closed_latencies, True),
                    get_open_scheduler(np.arange(len(open_latencies)), open_latencies, True)],
                    result_base_path + 'write-level-write-latency-burst-' + dist + '.pdf', ylimit=800,
                    ymin=0.00001,
                    post=post, logy=True)
  

process(uniform)
process(zipf)

