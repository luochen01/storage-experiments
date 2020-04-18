import xlrd 
import matplotlib.pyplot as plt
import numpy as np
from matplotlib import gridspec
from base import *

names = ['Age', 'Greedy', 'Cost-Benefit', 'Multi-Log', 'Multi-Log-Opt', "Min-Decline-Cost", "Min-Decline-Cost-Opt"]


def get_option(x, y, name):
    if name == 'Age':
        return PlotOption(x, y, name, linestyle='solid', marker='D', markevery=1, color='green')
    elif name == 'Greedy':
        return PlotOption(x, y, name, linestyle='solid', marker='s', markevery=1, color='blue')
    elif name == 'Cost-Benefit':
        return PlotOption(x, y, name, linestyle='solid', marker='^', markevery=1, color='dimgray')
    elif name == 'Multi-Log':
        return PlotOption(x, y, name, linestyle='solid', marker='X', markevery=1, color='orange')
    elif name == 'Multi-Log-Opt':
        return PlotOption(x, y, name, linestyle='dashed', marker='X', markevery=1, color='orange')
    elif name == 'Min-Decline-Cost':
        return PlotOption(x, y, name, linestyle='solid', marker='^', markevery=1, color='red')
    elif name == 'Min-Decline-Cost-Opt':
        return PlotOption(x, y, name, linestyle='dashed', marker='^', markevery=1, color='red')
    else:
        raise RuntimeError("Unknown name " + name)


write_path = "/Users/luochen/Desktop/book.xlsx"
output_path = "/Users/luochen/Documents/Research/papers/log-structure-gc/"

workbook = xlrd.open_workbook(write_path) 

factors = [0.5, 0.6, 0.7, 0.8, 0.9, 0.95]

xlabel = 'fill factor'
ylabel = 'cleaning overhead'


def plot_synthetic():
    fig, axs = plt.subplots(1, 3, figsize=(9, 2.5))
    for i in range(0, 3):
        axs[i].grid(True, axis='y', linestyle='dotted')
        
    sheet = workbook.sheet_by_name("synthetic")
    
    title_pos = -1
    
    options = []
    for i in range(0, len(names)):
        values = sheet.col_values((i + 1) * 3, 2, 8)
        options.append(get_option(params, values, names[i]))
    lines = plot_axis(axs[0], None, factors, 15, options, xlabel=xlabel + "\n(a) Uniform Distribution", ylabel=ylabel, ystep=2.5)

    options = []
    for i in range(0, len(names)):
        values = sheet.col_values((i + 1) * 3, 26, 32)
        options.append(get_option(params, values, names[i]))
    lines = plot_axis(axs[1], None, factors, 10, options, xlabel=xlabel + "\n(b)80-20 Zipfian Distribution", ylabel=ylabel, ystep=2.5)  

    options = []
    for i in range(0, len(names)):
        values = sheet.col_values((i + 1) * 3, 34, 40)
        options.append(get_option(params, values, names[i]))
    lines = plot_axis(axs[2], None, factors, 1.2, options, xlabel=xlabel + "\n(c) 90-10 Zipfian Distribution", ylabel=ylabel, ystep=0.2)

    fig.tight_layout(pad=0.0, w_pad=0.1)
    fig.legend(lines, labels=names, ncol=7, loc='upper center', borderpad=0)
    plt.subplots_adjust(top=0.9)
    
    path = output_path + "synthetic.pdf"
    plt.savefig(path)
    print('plotted ' + path)


def plot_tpcc():
    fig, ax = plt.subplots(1, 1, figsize=(3.5, 2.5))
    ax.grid(True, axis='y', linestyle='dotted')
        
    sheet = workbook.sheet_by_name("tpcc-btree")
    
    factors = [0.5, 0.6, 0.7, 0.8]
   
    options = []
    for i in range(0, len(names)):
        values = sheet.col_values((i + 1) * 3, 1, 5)
        options.append(get_option(params, values, names[i]))
    lines = plot_axis(ax, None, factors, 3.5, options, xlabel=xlabel, ylabel=ylabel, ystep=0.5)

    fig.tight_layout(pad=0.0, w_pad=0.1)
    ax.legend(lines, labels=names, ncol=1, loc='upper left')
    plt.subplots_adjust(top=1.0)
    
    path = output_path + "tpcc.pdf"
    plt.savefig(path)
    print('plotted ' + path)
   
def plot_sort():
    fig, ax = plt.subplots(1, 1, figsize=(3.5, 2.5))
    ax.grid(True, axis='y', linestyle='dotted')
        
    sheet = workbook.sheet_by_name("sort-batch-size")
    
    blocks = [0, 1, 4, 16, 64, 256, 1024 ]
    values = sheet.col_values(3, 1, 8)
    options = [get_option(blocks, values, 'Min-Decline-Cost')]
    lines = plot_axis(ax, None, blocks, 1.25, options, xlabel='write buffer size (#segments)', ylabel=ylabel, ystep=0.25, use_raw_value=False)
    
    path = output_path + "sort.pdf"
    plt.savefig(path)
    print('plotted ' + path)

    
plot_synthetic()
plot_tpcc()
plot_sort()
