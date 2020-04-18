import xlrd 
import matplotlib.pyplot as plt
import numpy as np

output_path = "/Users/luochen/Documents/Research/papers/lsm-memory/"

font_size = 10
font_weight = 100
label_size = 11
title_size = 11
legend_size = 11
params = {
    'font.family': 'Times New Roman',
    'font.weight': font_weight,
    'axes.labelweight': font_weight,
    'figure.titleweight': font_weight,
    'axes.titlesize': title_size,
   'axes.labelsize': label_size,
   'legend.fontsize': legend_size,
   'xtick.labelsize': label_size,
   'ytick.labelsize': label_size,
   'font.size': font_size,
   'lines.linewidth':1,
   'lines.markeredgewidth': 0,
   'lines.markersize':5,
   "legend.handletextpad":0.2,
   "legend.handlelength":1.5,
   'text.usetex': False,
   'savefig.bbox':'tight',
   'savefig.pad_inches':0,
   'figure.figsize':(3.25, 2.3),
   "legend.fancybox":True,
   "legend.shadow":False,
   "legend.framealpha":0,
   "legend.labelspacing":0.2,
   "legend.columnspacing":0.5,
   "legend.borderpad":0.2,
   "legend.borderaxespad":0,
   "hatch.color":'white',
   "hatch.linewidth":'0.5',
    "xtick.direction": 'out',
    "ytick.direction": 'out',
}
plt.rcParams.update(params)
plt.tight_layout()

xlabel_time = "Time (s)"
xlabel_memory = "Write Memory (GB)"
xlabel_total_memory = "Total Memory (GB)"
xlabel_skewness = "Skewness"

ylabel_throughput = "Throughput (kops/s)"
ylabel_transaction = "Throughput (ktxn/s)"
ylabel_transaction_write = "Disk Writes (KB/txn)"
ylabel_transaction_cost = "I/O Cost (KB/txn)"
ylabel_write_memory = "Write Memory (GB)"
ylabel_op_io = "I/O Cost (KB/op)"

btree_static = r"B$^+$-tree-static"
btree_static_default = r"B$^+$-tree-static-default"
btree_static_tuned = r"B$^+$-tree-static-tuned"
btree_dynamic = "B$^+$-tree-dynamic"
accordian_data = "Accordion-data"
accordion_index = "Accordion-index"
partitioned = "Partitioned"
partitioned_max_memory = "Partitioned-MEM"
partitioned_min_lsn = "Partitioned-LSN"
partitioned_opt = "Partitioned-OPT"

write_memory_values = ['1/8', '1/4', '1/2', '1', '2', '4', '8']
total_memory_values = ['4', '8', '12', '16', '20']
skew_values = ["50-50", "60-40", "70-30", "80-20", "90-10"]


class PlotOption(object):

    def __init__(self, x, y, legend='', color='black', linestyle='solid', marker=None, markevery=1, alpha=None, hatch=None, dashes=None):
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


def get_sub_sheet(sheet, row_begin, row_end, col_begin, col_end):
    rows = []
    for i in range(row_begin, row_end):
        rows.append(sheet.row_values(i, col_begin, col_end))
    return rows


write_path = "/Users/luochen/Documents/Research/experiments/results/memory/write-results.xlsx"
workbook = xlrd.open_workbook(write_path) 

tune_path = "/Users/luochen/Documents/Research/experiments/results/memory/tune-results.xlsx"
tune_workbook = xlrd.open_workbook(tune_path) 


def get_option(x, y, name):
    if name == btree_static_default or name == btree_static:
        return PlotOption(x, y, name, linestyle='dashed', marker='D', markevery=1, color='green')
    elif name == btree_static_tuned:
        return PlotOption(x, y, name, linestyle='dashed', marker='o', markevery=1, color='green')
    elif name == btree_dynamic:
        return PlotOption(x, y, name, linestyle='dashdot', marker='^', markevery=1, color='red')
    elif name == accordian_data:
        return PlotOption(x, y, name, linestyle='dotted', marker='P', markevery=1, color='blue')
    elif name == accordion_index:
        return PlotOption(x, y, name, linestyle='dotted', marker='X', markevery=1, color='blue')
    elif name == partitioned:
        return PlotOption(x, y, name, linestyle='solid', marker='s', markevery=1, color='dimgray')
    elif name == partitioned_max_memory:
        return PlotOption(x, y, name, linestyle='solid', marker='P', markevery=1, color='dimgray')
    elif name == partitioned_min_lsn:
        return PlotOption(x, y, name, linestyle='solid', marker='X', markevery=1, color='dimgray')
    elif name == partitioned_opt:
        return PlotOption(x, y, name, linestyle='solid', marker='s', markevery=1, color='dimgray')


def plot_axis(ax, name, xvalues, ylimit, options, xlabel=xlabel_memory, ylabel=ylabel_throughput, use_raw_value=False, ystep=None):
    x = np.arange(len(xvalues))
    i = 0
    lines = []
    for option in options:
        xvalue = x
        if use_raw_value:
            xvalue = option.x
        line = ax.plot(xvalue, option.y, label=option.legend, color=option.color, alpha=option.alpha, linestyle=option.linestyle, marker=option.marker, markevery=option.markevery)
        lines.append(line)
    ax.set_title(name)
    ax.set_xlabel(xlabel)
    ax.set_ylabel(ylabel)
    if use_raw_value:
        ax.set_xticks(xvalues)
    else:
        ax.set_xticks(x)
        ax.set_xticklabels(xvalues)
    ax.set_ylim(0, ylimit)
    if ystep != None:
        ax.set_yticks(np.arange(0, ylimit + 0.1, ystep))
    
    # ax.set_yscale("log")
    return lines

