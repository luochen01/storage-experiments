
import numpy as np
import pandas
import matplotlib.pyplot as plt
from base import *
from pathlib import PurePath
from itertools import count

ylimit = 18

settings.init()

write_window = 10


def get_eager(x, y):
    return PlotOption(x, y, color=red, legend='eager')


def get_lazy(x, y):
    return PlotOption(x, y, linestyle='solid', color=green, legend='lazy')


def parse_eager_util_latencies(base_path, utils):
    util_latencies = []
    for util in utils:
         (latencies, write_count) = parse_latencies(base_path + "write-secondary-open-95-eager-" + str(util) + ".log", "[Intended-UPDATE]")
         latencies = parse_latency_dists(latencies, write_count)
         util_latencies.append(latencies[3])
    return util_latencies


def process_query(base_path, selectivity, strategy, ylimit, title):

    def post():
        plt.title(title)
    
    df = open_csv(get_latest_file(base_path, "write-secondary-open-95-read-" + strategy + "-" + selectivity), header=1)
    fair_times = get_write_times(df, query_window)
    fair_queries = get_queries(df, query_window)
    
    df = open_csv(get_latest_file(base_path, "write-secondary-open-95-read-" + strategy + "-greedy-" + selectivity), header=1)
    greedy_times = get_write_times(df, query_window)
    greedy_queries = get_queries(df, query_window)
    
    if selectivity == '1' or selectivity == '10' or selectivity == '100':
        y_label = "Query Throughput (kops/s)"
        fair_queries = fair_queries / 1000
        greedy_queries = greedy_queries / 1000
        ylimit = ylimit / 1000
    else:
        y_label = "Query Throughput (10 ops/s)"
        fair_queries = fair_queries / 10
        greedy_queries = greedy_queries / 10
        ylimit = ylimit / 10    
        
    plot_queries([get_fair_scheduler(fair_times, fair_queries),
                  get_greedy_scheduler(greedy_times, greedy_queries)],
                  result_base_path + "write-secondary-read-" + strategy + "-" + selectivity + ".pdf", xstep=1800, ylimit=ylimit, post=post, ylabel=y_label)


def process(dist):
    secondary_base_path = base_path + dist + "/secondary/"
    print(secondary_base_path)
    df = open_csv(get_latest_file(secondary_base_path, 'write-secondary-lazy'), header=1)
    lazy_time = get_write_times(df, load_window)
    lazy_data = get_write_rates(df, load_window)
    
    df = open_csv(get_latest_file(secondary_base_path, 'write-secondary-eager'), header=1)
    eager_time = get_write_times(df, load_window)
    eager_data = get_write_rates(df, load_window)
    
    
    plot_writes([
        get_lazy(lazy_time, lazy_data),
        get_eager(eager_time, eager_data)], result_base_path + 'write-secondary-' + dist + '.pdf', ylimit=ylimit)
    
    df = open_csv(get_latest_file(secondary_base_path, 'write-secondary-open-95-lazy'), header=1)
    fair_time = get_write_times(df, write_window)
    fair_data = get_write_rates(df, write_window)
    
    df = open_csv(get_latest_file(secondary_base_path, 'write-secondary-open-95-lazy-greedy'), header=1)
    greedy_time = get_write_times(df, write_window)
    greedy_data = get_write_rates(df, write_window)
      
    def post():
        plt.legend(loc=1, ncol=1)
    
    plot_writes([
        get_fair_scheduler(fair_time, fair_data),
        get_greedy_scheduler(greedy_time, greedy_data)], result_base_path + 'write-secondary-open-lazy-' + dist + '.pdf',
        xstep=1800,
         ylimit=ylimit, post=post, ystep=3, title = '(a) Instantaneous Write\nThroughput')
    
    df = open_csv(get_latest_file(secondary_base_path, 'write-secondary-open-95-eager-7220'), header=1)
    fair_time = get_write_times(df, write_window)
    fair_data = get_write_rates(df, write_window)
    
    df = open_csv(get_latest_file(secondary_base_path, 'write-secondary-open-95-eager-greedy-7220'), header=1)
    greedy_time = get_write_times(df, write_window)
    greedy_data = get_write_rates(df, write_window)

    plot_writes([
        get_fair_scheduler(fair_time, fair_data),
        get_greedy_scheduler(greedy_time, greedy_data)],
        result_base_path + 'write-secondary-open-eager-' + dist + '.pdf',
        xstep=1800, title = '(a) Instantaneous Write\nThroughput',
        ylimit=ylimit, post=post, ystep=3)    
    
    def post():
        plt.legend(loc=2, ncol=1, bbox_to_anchor=None)
    
    (fair_latencies, write_count) = parse_latencies(secondary_base_path + "write-secondary-open-95-lazy.log", "[Intended-UPDATE]")
    fair_latencies = parse_latency_dists(fair_latencies, write_count)
      
    (greedy_latencies, write_count) = parse_latencies(secondary_base_path + "write-secondary-open-95-lazy-greedy.log", "[Intended-UPDATE]")
    greedy_latencies = parse_latency_dists(greedy_latencies, write_count)
    
    plot_latencies([
                    get_fair_scheduler(np.arange(len(fair_latencies)), fair_latencies, True),
                    get_greedy_scheduler(np.arange(len(greedy_latencies)), greedy_latencies, True)],
                    result_base_path + 'write-secondary-write-latency-lazy-' + dist + '.pdf',
                    post=post, logy=False, ylimit=0.4, title = '(b) Percentile Write\nLatencies')
    
    (fair_latencies, write_count) = parse_latencies(secondary_base_path + "write-secondary-open-95-eager-7220.log", "[Intended-UPDATE]")
    fair_latencies = parse_latency_dists(fair_latencies, write_count)
    
    (greedy_latencies, write_count) = parse_latencies(secondary_base_path + "write-secondary-open-95-eager-greedy-7220.log", "[Intended-UPDATE]")
    greedy_latencies = parse_latency_dists(greedy_latencies, write_count)
    
    def post():
        plt.legend(loc=4, ncol=1, bbox_to_anchor=None)
    
    plot_latencies([
                    get_fair_scheduler(np.arange(len(fair_latencies)), fair_latencies, True),
                    get_greedy_scheduler(np.arange(len(greedy_latencies)), greedy_latencies, True)],
                    result_base_path + 'write-secondary-write-latency-eager-' + dist + '.pdf',
                    post=post, logy=True, ylimit=300, ymin=0.01, title='(b) Percentile Write\nLatencies')

    settings.fig_size = (3.5, 2.5)

    speeds = [7220, 6840, 6460, 6080, 5700, 5320, 4940, 4560]
    utils = ['95%', '90%', '85%', '80%', '75%', '70%', '65%', '60%']
    util_latencies = parse_eager_util_latencies(secondary_base_path, speeds)
    
    def post():
       for i in range(0, len(speeds)):
           plt.text(i - 0.3, util_latencies[i] + 5, "{0:.2f}".format(util_latencies[i])) 
    
    plot_basic([PlotOption(utils, util_latencies, color='red', marker='s')],
        result_base_path + 'write-secondary-eager-util-latency.pdf', 'System Utilization', latency_ylabel, 1, xlimit=0, ylimit=150, xtick_labels=utils, logy=False, post=post)
    
    settings.fig_size = None

    
    secondary_base_path = base_path + dist + "/secondary-query/"
    
    sels = [1, 10, 100, 1000]
    ylimits = [10000, 3000, 400, 50]
    titles = ['Selectivity: 1 Record', 'Selectivity: 10 Records', 'Selectivity: 100 Records', 'Selectivity: 1000 Records']
    for i in range(0, 4):
        process_query(secondary_base_path, str(sels[i]), 'eager', ylimits[i], titles[i])
        process_query(secondary_base_path, str(sels[i]), 'lazy', ylimits[i], titles[i])


process(uniform)
# process(zipf)

