import xlrd 
import matplotlib.pyplot as plt
import numpy as np
from base import *

num_exprs = 7
names = ['Original', 'Grouped', 'Greedy-Grouped']


def get_option(x, y, name):
    if name == 'Original':
        return PlotOption(x, y, name, linestyle='dashed', marker='D', markevery=1, color='green')
    elif name == 'Grouped':
        return PlotOption(x, y, name, linestyle='dashdot', marker='s', markevery=1, color='red')
    elif name == 'Greedy-Grouped':
        return PlotOption(x, y, name, linestyle='dotted', marker='X', markevery=1, color='blue')


def plot_bar(ax, options):
    x = np.arange(len(write_memory_values))
    i = 0
    width = 0.25
    
    start = -width
    for option in options:
        ax.bar(x + start, option.y, width, label=option.legend, color=option.color, alpha=0.8)
        start += width
        
    ax.set_xlabel(xlabel_total_memory)
    ax.set_ylabel(ylabel_throughput)
    ax.set_xticks(x)
    ax.set_xticklabels(write_memory_values)
    ax.set_ylim(0, 80)
    #ax.set_xlim(-width * 2 - width / 2, len(write_memory_values) - 3.5 * width)
    ax.legend(ncol=3, loc='upper right')


def plot_l0():
    sheet = workbook.sheet_by_name("L0")
    fig, ax = plt.subplots(1, 1, figsize=(2.75, 2.25))
    pos = 1
    options = []
    i = 1
    for name in names:
        values = np.array(sheet.col_values(i, pos, pos + num_exprs)) / 1000
        options.append(get_option(params, values, name))
        i += 1
    #plot_bar(ax, options)
    lines = plot_axis(ax, None, write_memory_values, 80, options)
    ax.legend(lines, labels=names, ncol=1, loc='lower right')

    fig.tight_layout(pad=0, w_pad=0)

    path = output_path + "expr-l0.pdf"
    plt.savefig(path)
    print('plotted ' + path)


plot_l0()
