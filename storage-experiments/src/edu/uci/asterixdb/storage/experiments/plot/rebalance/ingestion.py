import xlrd 
import matplotlib.pyplot as plt
import numpy as np
from matplotlib import gridspec
from base import *

nodes = [2, 4, 8, 16]

legends = 3


def get_option(x, y, i):
    return PlotOption(x, y, names[i], color=colors[i])


def plot_bar(ax, options, ylimit, xlabel=xlabel_nodes):
    params = options[0].x
    x = np.arange(len(params))
    i = 0
    width = 0.2
    start = -width
    
    if len(options) == 1:
        start = 0
        width = 0.4
        
    for option in options:
        ax.bar(x + start, option.y, width, label=option.legend, color=option.color, alpha=1)
        start += width
    
    ax.set_xlabel(xlabel)
    ax.set_ylabel(ylabel_time)
    ax.set_xticks(x)
    ax.set_xticklabels(params)
    ax.set_ylim(0, ylimit)
    
    #if len(options) != 1:
        #ax.set_xlim(-width * 2 - width / 2, len(params) - width * 2 - width + width / 2)
    

def plot_ingestion():
    sheet = workbook.sheet_by_name("load time")
    fig, ax = plt.subplots(1)
    
    options = []
    pos = 1
    for i in range(0, legends):
        values = np.array(sheet.col_values(pos, 1, 5))
        pos += 1
        options.append(get_option(nodes, values, i))
        
    plot_bar(ax, options, 600)
    ax.legend(ncol=1, loc='upper left')

    path = output_path + "expr-ingestion.pdf"
    plt.savefig(path)
    print('plotted ' + path)
    
    
def plot_rebalance(mode):
    sheet = workbook.sheet_by_name("rebalance-" + mode)
    fig, ax = plt.subplots(1)
    
    options = []
    pos = 1
    for i in range(0, legends):
        values = np.array(sheet.col_values(pos, 1, 5))
        pos += 1
        options.append(get_option(nodes, values, i))
        
    plot_bar(ax, options, 600)
    ax.legend(ncol=1, loc='upper right')
    path = output_path + "expr-rebalance-" + mode + ".pdf"
    plt.savefig(path)
    print('plotted ' + path)

    
def plot_rebalance_write():
    sheet = workbook.sheet_by_name("rebalance-write")
    fig, ax = plt.subplots(1)
    
    write_speeds = [0, 10, 20, 30, 40]
    rebalance_times = [32.66, 45.18, 59.73, 89.46, 126.62]
    
    options = [get_option(write_speeds, rebalance_times, 2)]
    plot_bar(ax, options, 150, "Controlled Write Rate (krecords/s)")
    # ax.legend(ncol=1, loc='upper right')
    path = output_path + "expr-rebalance-write.pdf"
    plt.savefig(path)
    print('plotted ' + path)


plot_ingestion()
plot_rebalance("add")
plot_rebalance("remove")
plot_rebalance_write()
