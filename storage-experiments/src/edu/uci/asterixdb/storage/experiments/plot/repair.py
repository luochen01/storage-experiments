
import numpy as np
import pandas
import matplotlib as mt
import matplotlib.pyplot as plt
from base import *
from pathlib import PurePath

repair_base_path = base_path + 'repair/'

print(repair_base_path)


class RepairResult(object):

    def __init__(self, csv):
        self.total_records = csv['records'] / 1000000
        self.time = csv['time'] / 1000


def open_csv(path):
    try:
        csv = pandas.read_csv(path, sep='\t', header=None, names=['records', 'time'])
        return RepairResult(csv)
    except:
        print('fail to parse ' + path + sys.exc_info()[0])
        return None


repair_dataset_0 = open_csv(repair_base_path + 'repair-dataset-UNIFORM-0.csv')
repair_dataset_5 = open_csv(repair_base_path + 'repair-dataset-UNIFORM-0.5.csv')
repair_datasets = [repair_dataset_0, repair_dataset_5]

repair_dataset_compact_0 = open_csv(repair_base_path + 'repair-dataset-compact-UNIFORM-0.csv')
repair_dataset_compact_5 = open_csv(repair_base_path + 'repair-dataset-compact-UNIFORM-0.5.csv')
repair_dataset_compacts = [repair_dataset_compact_0, repair_dataset_compact_5]

repair_index_0 = open_csv(repair_base_path + 'repair-index-UNIFORM-0-prefix.csv')
repair_index_5 = open_csv(repair_base_path + 'repair-index-UNIFORM-0.5-prefix.csv')
repair_indexes = [repair_index_0, repair_index_5]

repair_index_bf_0 = open_csv(repair_base_path + 'repair-index-UNIFORM-0-bf.csv')
repair_index_bf_5 = open_csv(repair_base_path + 'repair-index-UNIFORM-0.5-bf.csv')
repair_index_bfs = [repair_index_bf_0, repair_index_bf_5]

repair_dataset_record_500 = open_csv(repair_base_path + 'repair-dataset-UNIFORM-0.1-500.csv')
repair_dataset_record_1000 = open_csv(repair_base_path + 'repair-dataset-UNIFORM-0.1-1000.csv')
repair_dataset_record_2000 = open_csv(repair_base_path + 'repair-dataset-UNIFORM-0.1-2000.csv')
repair_dataset_records = [repair_dataset_record_500, repair_dataset_record_1000, repair_dataset_record_2000]

repair_index_record_500 = open_csv(repair_base_path + 'repair-index-UNIFORM-0.1-500.csv')
repair_index_record_1000 = open_csv(repair_base_path + 'repair-index-UNIFORM-0.1-1000.csv')
repair_index_record_2000 = open_csv(repair_base_path + 'repair-index-UNIFORM-0.1-2000.csv')
repair_index_records = [repair_index_record_500, repair_index_record_1000, repair_index_record_2000]

repair_index_bf_record_500 = open_csv(repair_base_path + 'repair-index-bf-UNIFORM-0.1-500.csv')
repair_index_bf_record_1000 = open_csv(repair_base_path + 'repair-index-bf-UNIFORM-0.1-1000.csv')
repair_index_bf_record_2000 = open_csv(repair_base_path + 'repair-index-bf-UNIFORM-0.1-2000.csv')
repair_index_bf_records = [repair_index_bf_record_500, repair_index_bf_record_1000, repair_index_bf_record_2000]

repair_dataset_index_1 = open_csv(repair_base_path + 'repair-dataset-UNIFORM-0.1-500.csv')
repair_dataset_index_3 = open_csv(repair_base_path + 'repair-dataset-index-3.csv')
repair_dataset_index_5 = open_csv(repair_base_path + 'repair-dataset-index-5.csv')
repair_dataset_indexes = [repair_dataset_index_1, repair_dataset_index_3, repair_dataset_index_5]

repair_index_index_1 = open_csv(repair_base_path + 'repair-index-UNIFORM-0.1-500.csv')
repair_index_index_3 = open_csv(repair_base_path + 'repair-index-index-3.csv')
repair_index_index_5 = open_csv(repair_base_path + 'repair-index-index-5.csv')
repair_index_indexes = [repair_index_index_1, repair_index_index_3, repair_index_index_5]

repair_index_bf_index_1 = open_csv(repair_base_path + 'repair-index-bf-UNIFORM-0.1-500.csv')
repair_index_bf_index_3 = open_csv(repair_base_path + 'repair-index-index-bf-3.csv')
repair_index_bf_index_5 = open_csv(repair_base_path + 'repair-index-index-bf-5.csv')
repair_index_bf_indexes = [repair_index_bf_index_1, repair_index_bf_index_3, repair_index_bf_index_5]


def plot_repair(options, output, xlabel='Total Ingested Records (millions)', ylabel='Repair Time (s)', xlimit=110, ylimit=1100):
    # use as global

    plt.figure()
    for option in options:
        plt.plot(option.data.total_records, option.data.time, label=option.legend, color=option.color, linestyle=option.linestyle,
                  markerfacecolor='none', markeredgecolor=option.color, marker=option.marker, markevery=1,
                  linewidth=1.0)

    legend_col = 1
    plt.legend(loc=2, ncol=legend_col)

    plt.xlabel(xlabel)
    plt.xticks(np.arange(0, xlimit, step=10))
    plt.xlim(0, xlimit)
    plt.ylim(0, ylimit)
    plt.ylabel(ylabel)
    plt.gca().yaxis.grid(linestyle='dotted')
    plt.savefig(output)
    print('output figure to ' + output)


def plot_options(options, ax, title, xlabel, xlimit, ylimit):
    lines = []
    for option in options:
        line, = ax.plot(option.data.total_records, option.data.time, label=option.legend, color=option.color, linestyle=option.linestyle,
                  markerfacecolor='none', markeredgecolor=option.color, marker=option.marker, markevery=1,
                  linewidth=1.0)
        lines.append(line)
    ax.set_title(title)
    ax.set_xlabel(xlabel)
    ax.set_xticks(np.arange(0, xlimit, step=20))
    ax.set_xlim(0, xlimit)
    ax.set_ylim(0, ylimit)
    ax.yaxis.grid(linestyle='dotted')
    return lines

def plot_shared_repair(options_1, options_2, options_3, titles, output, xlabel='Total Ingested Records (millions)', ylabel='Repair Time (s)', xlimit=110, ylimit=1100):
    # use as global

    f, (ax1, ax2, ax3) = plt.subplots(1, 3, sharey=True, figsize=(9, 3))
    plt.subplots_adjust(wspace=0.05, hspace=0)
    lines = plot_options(options_1, ax1, titles[0] , "", xlimit, ylimit)
    plot_options(options_2, ax2, titles[1], xlabel, xlimit, ylimit)
    plot_options(options_3, ax3, titles[2], "", xlimit, ylimit)

    legend_col = 1
    f.legend(handles = lines, loc='upper left', ncol=2, bbox_to_anchor=(0.08,0.95), shadow=False, framealpha=0)

    #ax1.legend(loc=2, ncol=legend_col)
    ax1.set_ylabel(ylabel)

    plt.savefig(output)
    print('output figure to ' + output)


updates = [0, 0.5]
for i in range(0, 2):
    plot_repair([PlotOption(repair_datasets[i], 'primary repair (scan)', marker=markers[0], linestyle=antimatter_linestyle, color=antimatter_color),
                 PlotOption(repair_dataset_compacts[i], 'primary repair (merge)', marker=markers[1], linestyle=validation_norepair_linestyle, color=validation_norepair_color),
                 PlotOption(repair_indexes[i], 'secondary repair', marker=markers[2], linestyle=validation_linestyle, color=validation_color),
                 PlotOption(repair_index_bfs[i], 'secondary repair (bloom filter)', marker=markers[3], linestyle=inplace_linestyle, color=inplace_color)],
                result_base_path + 'repair-' + str(updates[i]) + '.pdf')

record_sizes = [500, 1000, 2000]
indexes = [1, 3, 5]
index_options = []
record_options = []
for i in range (0, 3):
    record_options.append([PlotOption(repair_dataset_records[i], 'primary repair (scan)', marker=markers[0], linestyle=antimatter_linestyle, color=antimatter_color),
                 PlotOption(repair_index_records[i], 'secondary repair', marker=markers[2], linestyle=validation_linestyle, color=validation_color),
                 PlotOption(repair_index_bf_records[i], 'secondary repair (bloom filter)', marker=markers[3], linestyle=inplace_linestyle, color=inplace_color)])
    index_options.append([PlotOption(repair_dataset_indexes[i], 'primary repair (scan)', marker=markers[0], linestyle=antimatter_linestyle, color=antimatter_color),
                    PlotOption(repair_index_indexes[i], 'secondary repair', marker=markers[2], linestyle=validation_linestyle, color=validation_color),
                    PlotOption(repair_index_bf_indexes[i], 'secondary repair (bloom filter)', marker=markers[3], linestyle=inplace_linestyle, color=inplace_color)])

plot_shared_repair(record_options[0], record_options[1], record_options[2], ['500 Bytes', '1KB', '2KB'], result_base_path + 'repair-record-size.pdf', ylimit=3000)
plot_shared_repair(index_options[0], index_options[1], index_options[2], ['1 Index', '3 Indexes', '5 Indexes'], result_base_path + 'repair-index.pdf', ylimit=850)
