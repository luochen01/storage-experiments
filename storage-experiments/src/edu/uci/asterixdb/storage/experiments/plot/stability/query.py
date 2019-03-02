from itertools import count
from pathlib import PurePath

import pandas

from base import *
import matplotlib.pyplot as plt
import numpy as np
import settings

settings.init()

def process_query(base_path, fair_input, greedy_input, output, ylabel, window):
    df = open_csv(get_latest_file(base_path, fair_input), header=1)
    fair_times = get_write_times(df, window)
    fair_queries = get_queries(df, window)
    
    df = open_csv(get_latest_file(base_path, greedy_input), header=1)
    greedy_times = get_write_times(df, window)
    greedy_queries = get_queries(df, window)
    
    if 'long' in fair_input:
        fair_queries = fair_queries * 60
        greedy_queries = greedy_queries * 60

    if 'read' in fair_input:
        fair_queries = fair_queries / 1000
        greedy_queries = greedy_queries / 1000
    
    plot_queries([get_fair_scheduler(fair_times, fair_queries),
                  get_greedy_scheduler(greedy_times, greedy_queries)],
                  result_base_path + output, ylabel)


tier_items = [
    ['write-tier-open-95-read', 'write-tier-open-95-read-greedy', 'query-tier-point.pdf', point_ylabel, query_window],
    ['write-tier-open-95-scan', 'write-tier-open-95-scan-greedy', 'query-tier-scan.pdf', short_ylabel, query_window],
    ['write-tier-open-95-long', 'write-tier-open-95-long-greedy', 'query-tier-long.pdf', long_ylabel, 60],
]

level_items = [
    ['write-level-open-95-read', 'write-level-open-95-read-greedy', 'query-level-point.pdf', point_ylabel, query_window],
    ['write-level-open-95-scan', 'write-level-open-95-scan-greedy', 'query-level-scan.pdf', short_ylabel, query_window],
    ['write-level-open-95-long', 'write-level-open-95-long-greedy', 'query-level-long.pdf', long_ylabel, 60],
]


def process(dist):
    tier_base_path = base_path + dist + "/tier-query/"
    print(tier_base_path)
    
    for item in tier_items:
        process_query(tier_base_path, item[0], item[1], item[2], item[3], item[4])
        
    level_base_path = base_path + dist + "/level-query/"
    print(level_base_path)
    
    for item in level_items:
        process_query(level_base_path, item[0], item[1], item[2], item[3], item[4])
  

process(uniform)

