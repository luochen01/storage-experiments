import xlrd 
import matplotlib.pyplot as plt
import numpy as np
from matplotlib import gridspec
from base import *

tuned = "tuned"
optimal = "opt"
static_128MB = "64M"
static_half = "50%"

names = [tuned, optimal, static_128MB, static_half]


def get_option(x, y, name, suffix, hatch):
    fullname = name
    if suffix != None:
        fullname = name + "-" + suffix
    if name == tuned:
        return PlotOption(x, y, fullname, linestyle='solid', marker='D', markevery=1, color='green', hatch=hatch)
    elif name == optimal:
        return PlotOption(x, y, fullname, linestyle='solid', marker='o', markevery=1, color='red', hatch=hatch)
    elif name == static_128MB:
        return PlotOption(x, y, fullname, linestyle='solid', marker='^', markevery=1, color='blue', hatch=hatch)
    elif name == static_half:
        return PlotOption(x, y, fullname, linestyle='solid', marker='P', markevery=1, color='orange', hatch=hatch)


width = 0.2


def plot_io_axis(ax, options):
    x = np.arange(len(total_memory_values))
    i = 0

    start = -width * 2 + width / 2
    for array in options:
        ax.bar(x + start, array[0].y, width, label=array[0].legend, color=array[0].color, alpha=1)
        start += width
    
    start = -width * 2 + width / 2
    for array in options:
        ax.bar(x + start, array[1].y, width, bottom=array[0].y, label=array[1].legend, color=array[1].color, alpha=0.5)
        start += width
        
    ax.set_xlabel(xlabel_total_memory + "\n\n" + '(a) Weighted I/O Cost')
    ax.set_ylabel(ylabel_transaction_weighted_cost)
    ax.set_xticks(x)
    ax.set_xticklabels(total_memory_values)
    ax.set_ylim(0, 4.5)
    ax.set_xlim(-width * 2 - width/2, len(total_memory_values) -width * 2 -  width +width/2)
    ax.legend(ncol=2, loc='upper right', columnspacing=0.15, labelspacing=0.05, handlelength=1.15)

    
def plot_txn_axis(ax, options):
    x = np.arange(len(total_memory_values))
    i = 0

    start = -width * 2 + width / 2
    for array in options:
        ax.bar(x + start, array[0].y, width, label=array[0].legend, color=array[0].color, alpha=0.8)
        start += width
    
    ax.set_xlabel(xlabel_total_memory + "\n\n" + '(b) Transaction Throughput')
    ax.set_ylabel(ylabel_transaction)
    ax.set_xticks(x)
    ax.set_xticklabels(total_memory_values)
    ax.set_ylim(0, 4.5)
    ax.legend(ncol=4, loc='upper right', columnspacing=0.15, labelspacing=0.05, handlelength=1.15)


def plot_tpcc():
    fig, axs = plt.subplots(1, 2, figsize=(5.5, 2.5))

    sheet = tune_workbook.sheet_by_name("tpcc-tune-io")
    
    options = []
    for i in range(0, len(names)):
        writes = np.array(sheet.col_values(i * 4 + 2, 2, 7)) / 100
        reads = np.array(sheet.col_values(i * 4 + 3, 2, 7)) / 100
        array = [get_option(params, writes, names[i], "write", "/"), get_option(params, reads, names[i], "read", "+")]
        options.append(array)
    lines = plot_io_axis(axs[0], options)
    
    options = []
    for i in range(0, len(names)):
        txns = np.array(sheet.col_values(i * 4 + 1, 2, 7)) / 1000
        array = [get_option(params, txns, names[i], None, "/")]
        options.append(array)
    lines = plot_txn_axis(axs[1], options)
   
    fig.tight_layout(pad=0.0, w_pad = 1.5)
    plt.subplots_adjust()
    
    path = output_path + "expr-tune-tpcc.pdf"
    plt.savefig(path)
    print('plotted ' + path)


plot_tpcc()
