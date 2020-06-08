import xlrd 
import matplotlib.pyplot as plt
import numpy as np
from matplotlib import gridspec
from base import *

tuned = "tuned"
optimal = "opt"
static_64MB = "64MB"
static_half = "50%"

names = [tuned, optimal, static_64MB, static_half]


output_path = "/Users/luochen/Desktop/tmp/"
font_size= 12

params = {
    'font.family': 'Calibri',
}
plt.rcParams.update(params)



def get_option(x, y, name, suffix, hatch):
    fullname = name + "-" + suffix
    if name == tuned:
        return PlotOption(x, y, fullname, linestyle='solid', marker='D', markevery=1, color='green', hatch=hatch)
    elif name == optimal:
        return PlotOption(x, y, fullname, linestyle='solid', marker='o', markevery=1, color='red', hatch=hatch)
    elif name == static_64MB:
        return PlotOption(x, y, fullname, linestyle='solid', marker='^', markevery=1, color='blue', hatch=hatch)
    elif name == static_half:
        return PlotOption(x, y, fullname, linestyle='solid', marker='P', markevery=1, color='orange', hatch=hatch)


def plot_axis(ax, options):
    x = np.arange(len(total_memory_values))
    i = 0

    width = 0.2
    start = -width * 2 + width / 2
    for array in options:
        ax.bar(x + start, array[0].y, width, label=array[0].legend, color=array[0].color, alpha=1)
        start += width
    
    start = -width * 2 + width / 2
    for array in options:
        ax.bar(x + start, array[1].y, width, bottom=array[0].y, label=array[1].legend, color=array[1].color, alpha=0.5)
        start += width
        
    ax.set_xlabel(xlabel_total_memory)
    ax.set_ylabel(ylabel_transaction_cost)
    ax.set_xticks(x)
    ax.set_xticklabels(total_memory_values)
    ax.set_ylim(0, 260)
        

def plot_tpcc():
    fig, axs = plt.subplots(1, 1, figsize=(6, 3))

    sheet = tune_workbook.sheet_by_name("tpcc-tune-io")
    
    options = []
    for i in range(0, len(names)):
        writes = np.array(sheet.col_values(i * 4 + 2, 2, 7))
        reads = np.array(sheet.col_values(i * 4 + 3, 2, 7))
        array = [get_option(params, writes, names[i], "write", "/"), get_option(params, reads, names[i], "read", "+")]
        options.append(array)
    lines = plot_axis(axs, options)
   
    fig.tight_layout(pad=0.0)
    fig.legend(ncol=2, loc='upper right', columnspacing=0.2, labelspacing=0.05)
    plt.subplots_adjust()
    
    path = output_path + "expr-tune-tpcc.pdf"
    plt.savefig(path)
    print('plotted ' + path)


plot_tpcc()
