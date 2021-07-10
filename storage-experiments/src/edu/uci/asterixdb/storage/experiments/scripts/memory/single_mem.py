import xlrd 
import matplotlib.pyplot as plt
import numpy as np
from base import *

num_exprs = 4
btree = btree_dynamic
lsm = partitioned

names = [lsm, btree]
workloads = ['Write\nOnly', 'Write\nHeavy', 'Read\nHeavy', 'Scan\nHeavy']

params = {
   'xtick.labelsize': 9,
}
#plt.rcParams.update(params)
plt.tight_layout()


output_path = "/Users/luochen/Desktop/tmp/"


def get_option(x, y, name):
    if name == lsm:
        return PlotOption(x, y, name, linestyle='dashed', marker='D', markevery=1, color='gray')
    elif name == btree:
        return PlotOption(x, y, name, linestyle='dashdot', marker='s', markevery=1, color='red')


def plot_bar(ax, options):
    x = np.arange(len(workloads))
    i = 0
    width = 0.2
    
    start = -0.5 * width
    for option in options:
        ax.bar(x + start, option.y, width, label=option.legend, color=option.color, alpha=1.0)
        start += width
        
    ax.set_xlabel(None)
    ax.set_ylabel(ylabel_throughput)
    ax.set_xticks(x)
    ax.set_xticklabels(workloads)
    ax.set_ylim(0, 120)
    #ax.set_xlim(-1.5 * width , len(workloads) - 4 * width)
    ax.legend(ncol=1, loc='upper left')


def plot_memory():
    sheet = workbook.sheet_by_name("memory")
    fig, ax = plt.subplots(1, 1, figsize=(2.75, 2.25))
    pos = 1
    options = []
    i = 1
    for name in names:
        values = np.array(sheet.col_values(i, pos, pos + num_exprs)) / 1000
        options.append(get_option(params, values, name))
        i += 1
    plot_bar(ax, options)
    path = output_path + "expr-memory-1.pdf"
    plt.savefig(path)
    print('plotted ' + path)


plot_memory()
