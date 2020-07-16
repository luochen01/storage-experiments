import xlrd 
import matplotlib.pyplot as plt
import numpy as np
from base import *

adaptive = 'dynamic'
static_32MB = 'static-32MB'
static_1GB = 'static-1GB'

names = [adaptive, static_32MB, static_1GB]

xvalues = range(300, 7500, 300)
xticks = range(0, 7500, 1800)

ylimit = 60


def get_option(x, y, name):
    if name == static_32MB:
        return PlotOption(x, y, name, linestyle='dashed', marker='D', markevery=2, color='red')
    elif name == static_1GB:
        return PlotOption(x, y, name, linestyle='dashed', marker='o', markevery=2, color='green')
    elif name == adaptive:
        return PlotOption(x, y, name, linestyle='solid', marker='s', markevery=2, color='dimgray')


def plot_single_dynamic():
    sheet = workbook.sheet_by_name("single-dynamic")
    options = []
    col = 1
    for i in range(0, len(names)):
        values = np.array(sheet.col_values(col, 1, 26)) / 1000
        option = get_option(xvalues, values, names[i])
        options.append(option)
        col += 2
    
    # x = np.arange(len(xvalues))
    fig, ax = plt.subplots(figsize=(2.75, 2.25))
    fig.tight_layout(pad=0.0)

    for i in range(0, len(names)):
        option = options[len(names) - i - 1]
        ax.plot(xvalues, option.y, label=option.legend, color=option.color, alpha=option.alpha, linestyle=option.linestyle, marker=option.marker, markevery=option.markevery)
    ax.set_xlabel(xlabel_time)
    ax.set_ylabel(ylabel_throughput)
    ax.set_xticks(xticks)
    ax.set_ylim(0, ylimit)
    
    #ax.vlines(1800, 0, 60, linestyle='dashed')
    ax.text(1000, 25, '1GB=>32MB')
    
    #ax.vlines(3600, 0, 60, linestyle='dashed')
    ax.text(2500, 18, '32MB=>1GB')
    
    #ax.vlines(5400, 0, 60, linestyle='dashed')
    ax.text(4600, 25, '1GB=>32MB')
    
    ax.legend(loc='upper right', framealpha=0.5)
    
    # ax.set_xlim(0, len(x))
    fig.tight_layout(pad=0, w_pad=0)
    path = output_path + "expr-single-dynamic.pdf"
    plt.savefig(path)
    print("plotted " + path)

                
plot_single_dynamic()
