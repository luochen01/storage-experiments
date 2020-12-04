import xlrd 
import matplotlib.pyplot as plt
import numpy as np
from matplotlib import gridspec
from base import *


def get_option(x, y, i):
    return PlotOption(x, y, names[i], color=colors[i])


def plot_bar(ax, options, ylimit):
    params = options[0].x
    x = np.arange(len(params))
    i = 0
    width = 0.2
    start = 0
    if len(options) == 4:
        start = -width * 1.5
    else:
        start = -width 
    for option in options:
        ax.bar(x + start, option.y, width, label=option.legend, color=option.color, alpha=1)
        start += width
    
    ax.set_ylabel(ylabel_time_sec)
    ax.set_xticks(x)
    ax.set_xticklabels(params)
    ax.set_ylim(0, ylimit)
    ax.set_xlim(-width * 2 - width / 2, len(params) - width * 2 - width + width / 2)

def plot_query(nodes, legends, ylimit = 6000):
    sheet = workbook.sheet_by_name("query-" + nodes)
    fig, ax = plt.subplots(1, figsize=(8, 2.3))
    
    queries = sheet.col_values(0, 1, 23)
    options = []
    for i in range(0, legends):
        values = np.array(sheet.col_values(i + 1, 1, 23))
        options.append(get_option(queries, values, i))
        
    plot_bar(ax, options, ylimit)
    ax.legend(ncol=4, loc='upper left')

    path = output_path + "expr-query-" + nodes + ".pdf"
    plt.savefig(path)
    print('plotted ' + path)
    

plot_query("4", 4)
plot_query("16", 4)
plot_query("3", 3, 7500)
plot_query("15", 3, 7500)
