
import numpy as np
import pandas
import matplotlib as mt
import matplotlib.pyplot as plt
from base import *
from pathlib import PurePath

index = hdd_index

repair_base_path = base_path + devices[index] + '/repair/'

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

repair_index_0 = open_csv(repair_base_path + 'repair-index-UNIFORM-0.csv')
repair_index_5 = open_csv(repair_base_path + 'repair-index-UNIFORM-0.5.csv')
repair_indexes = [repair_index_0, repair_index_5]

repair_index_nobf_0 = open_csv(repair_base_path + 'repair-index-UNIFORM-0-nobf.csv')
repair_index_nobf_5 = open_csv(repair_base_path + 'repair-index-UNIFORM-0.5-nobf.csv')
repair_index_nobfs = [repair_index_nobf_0, repair_index_nobf_5]

# repair_dataset_record_500 = open_csv(repair_base_path + 'repair-dataset-UNIFORM-0.1-500.csv')
repair_dataset_record_1000 = open_csv(repair_base_path + 'repair-dataset-UNIFORM-0.1-1000.csv')
# repair_dataset_record_2000 = open_csv(repair_base_path + 'repair-dataset-UNIFORM-0.1-2000.csv')
repair_dataset_records = [ repair_dataset_record_1000]

# repair_index_record_500 = open_csv(repair_base_path + 'repair-index-UNIFORM-0.1-500.csv')
repair_index_record_1000 = open_csv(repair_base_path + 'repair-index-UNIFORM-0.1-1000.csv')
# repair_index_record_2000 = open_csv(repair_base_path + 'repair-index-UNIFORM-0.1-2000.csv')
repair_index_records = [repair_index_record_1000]

# repair_index_bf_record_500 = open_csv(repair_base_path + 'repair-index-bf-UNIFORM-0.1-500.csv')
repair_index_nobf_record_1000 = open_csv(repair_base_path + 'repair-index-nobf-UNIFORM-0.1-1000.csv')
# repair_index_bf_record_2000 = open_csv(repair_base_path + 'repair-index-bf-UNIFORM-0.1-2000.csv')
repair_index_nobf_records = [repair_index_nobf_record_1000]

# repair_dataset_index_1 = open_csv(repair_base_path + 'repair-dataset-UNIFORM-0.1-500.csv')
# repair_dataset_index_3 = open_csv(repair_base_path + 'repair-dataset-index-3.csv')
repair_dataset_index_5 = open_csv(repair_base_path + 'repair-dataset-index-5.csv')
repair_dataset_indexes = [repair_dataset_index_5]

# repair_index_index_1 = open_csv(repair_base_path + 'repair-index-UNIFORM-0.1-500.csv')
# repair_index_index_3 = open_csv(repair_base_path + 'repair-index-index-3.csv')
repair_index_index_5 = open_csv(repair_base_path + 'repair-index-index-5.csv')
repair_index_indexes = [repair_index_index_5]

# repair_index_bf_index_1 = open_csv(repair_base_path + 'repair-index-bf-UNIFORM-0.1-500.csv')
# repair_index_bf_index_3 = open_csv(repair_base_path + 'repair-index-index-bf-3.csv')
repair_index_nobf_index_5 = open_csv(repair_base_path + 'repair-index-index-nobf-5.csv')
repair_index_nobf_indexes = [ repair_index_nobf_index_5]


def plot_repair(options, output, xlabel='Total Ingested Records (millions)', ylabel='Repair Time (s)', xlimit=105, ylimit=0):
    # use as global

    plt.figure()
    for option in options:
        plt.plot(option.data.total_records, option.data.time, label=option.legend, color=option.color, linestyle=option.linestyle,
                  markerfacecolor='none', markeredgecolor=option.color, marker=option.marker, markevery=(1, 1), markersize=5)

    legend_col = 1
    plt.legend(loc=2, ncol=legend_col)

    plt.xlabel(xlabel)
    plt.xticks(np.arange(0, xlimit, step=20))
    plt.xlim(5, xlimit)
    if ylimit > 0:
        plt.ylim(0, ylimit)
    plt.ylabel(ylabel)
    plt.savefig(output)
    print('output figure to ' + output)


def plot_options(options, ax, title, xlabel, xlimit, ylimit):
    lines = []
    for option in options:
        line, = ax.plot(option.data.total_records, option.data.time, label=option.legend, color=option.color, linestyle=option.linestyle,
                  markerfacecolor='none', markeredgecolor=option.color, marker=option.marker, markevery=(1, 1),
                  linewidth=1.0, markersize=5)
        lines.append(line)
    ax.set_title(title)
    ax.set_xlabel(xlabel)
    ax.set_xticks(np.arange(0, xlimit, step=20))
    ax.set_xlim(5, xlimit)
    if ylimit > 0:
        ax.set_ylim(0, ylimit)
        ax.set_yticks(np.arange(0, ylimit, step=250))
    return lines


def plot_shared_repair(options, titles, output, xlabel='Total Records (Millions)', ylabel='Repair Time (s)', xlimit=105, ylimit=0,
                       figsize=(0, 0), bbox_to_anchor=(0, 0), colspace=None, ncols=2):
    # use as global
    num = len(options)
    f, axes = plt.subplots(1, num, sharey=True, figsize=figsize)
    plt.subplots_adjust(wspace=0.05, hspace=0)
    for i in range(0, num):
        ax_xlabel = xlabel
        if i % 2 != 1 & num % 2 == 1:
            ax_xlabel = ""
        lines = plot_options(options[i], axes[i], titles[i], ax_xlabel, xlimit, ylimit)

    axes[0].legend(framealpha=0.5)
    axes[1].legend(framealpha=0.5)

    axes[0].set_ylabel(ylabel)

    plt.savefig(output)
    print('output figure to ' + output)


updates = [0, 0.5]
update_options = [
    [PlotOption(repair_datasets[0], 'primary repair', marker=markers[0], linestyle=antimatter_linestyle, color=antimatter_color),
                 PlotOption(repair_dataset_compacts[0], 'primary repair (merge)', marker=markers[1], linestyle=validation_norepair_linestyle, color=validation_norepair_color),
                 PlotOption(repair_indexes[0], 'secondary repair', marker=markers[2], linestyle=validation_linestyle, color=validation_color),
                 PlotOption(repair_index_nobfs[0], 'secondary repair (nobf)', marker=markers[3], linestyle=inplace_linestyle, color=inplace_color)],
    [PlotOption(repair_datasets[1], 'primary repair', marker=markers[0], linestyle=antimatter_linestyle, color=antimatter_color),
                 PlotOption(repair_dataset_compacts[1], 'primary repair (merge)', marker=markers[1], linestyle=validation_norepair_linestyle, color=validation_norepair_color),
                 PlotOption(repair_indexes[1], 'secondary repair', marker=markers[2], linestyle=validation_linestyle, color=validation_color),
                 PlotOption(repair_index_nobfs[1], 'secondary repair (nobf)', marker=markers[3], linestyle=inplace_linestyle, color=inplace_color)]
    ]

record_sizes = [500, 1000, 2000]
indexes = [1, 3, 5]
index_options = []
record_options = []
for i in range (0, 1):
    record_options.append([PlotOption(repair_dataset_records[i], 'primary repair', marker=markers[0], linestyle=antimatter_linestyle, color=antimatter_color),
                 PlotOption(repair_index_records[i], 'secondary repair', marker=markers[2], linestyle=validation_linestyle, color=validation_color),
                 PlotOption(repair_index_nobf_records[i], 'secondary repair (nobf)', marker=markers[3], linestyle=inplace_linestyle, color=inplace_color)])
    index_options.append([PlotOption(repair_dataset_indexes[i], 'primary repair', marker=markers[0], linestyle=antimatter_linestyle, color=antimatter_color),
                    PlotOption(repair_index_indexes[i], 'secondary repair', marker=markers[2], linestyle=validation_linestyle, color=validation_color),
                    PlotOption(repair_index_nobf_indexes[i], 'secondary repair (nobf)', marker=markers[3], linestyle=inplace_linestyle, color=inplace_color)])

plot_shared_repair(update_options, ['Update Ratio 0%', 'Update Ratio 50%'], result_base_path + devices[index] + '-repair-update.pdf', figsize=(6, 2.2))
# plot_shared_repair(record_options, ['500 Bytes', '1KB', '2KB'], result_base_path + 'repair-record-size.pdf', ncols=1)
# plot_shared_repair(index_options, ['1 Index', '3 Indexes', '5 Indexes'], result_base_path + 'repair-index.pdf', bbox_to_anchor=(0.075, 1), colspace=0.75)

plot_repair([PlotOption(repair_dataset_records[0], 'primary repair', marker=markers[0], linestyle=antimatter_linestyle, color=antimatter_color),
                 PlotOption(repair_index_records[0], 'secondary repair', marker=markers[2], linestyle=validation_linestyle, color=validation_color),
                 PlotOption(repair_index_nobf_records[0], 'secondary repair (nobf)', marker=markers[3], linestyle=inplace_linestyle, color=inplace_color)],
                 result_base_path + devices[index] + '-repair-record-size.pdf')

plot_repair([PlotOption(repair_dataset_indexes[0], 'primary repair', marker=markers[0], linestyle=antimatter_linestyle, color=antimatter_color),
                    PlotOption(repair_index_indexes[0], 'secondary repair', marker=markers[2], linestyle=validation_linestyle, color=validation_color),
                    PlotOption(repair_index_nobf_indexes[0], 'secondary repair (nobf)', marker=markers[3], linestyle=inplace_linestyle, color=inplace_color)],
                 result_base_path + devices[index] + '-repair-index.pdf')

