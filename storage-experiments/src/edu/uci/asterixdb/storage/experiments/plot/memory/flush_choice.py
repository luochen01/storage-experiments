import xlrd 
import matplotlib.pyplot as plt
import numpy as np
from base import *

num_exprs = 7
names = ['Round-Robin', 'Oldest', 'Full', 'Adaptive']

def get_option(x, y, name):
    if name == 'Adaptive':
        return PlotOption(x, y, name, linestyle='dashed', marker='D', markevery=1, color='green')
    elif name == 'Round-Robin':
        return PlotOption(x, y, name, linestyle='dashdot', marker='s', markevery=1, color='red')
    elif name == 'Oldest':
        return PlotOption(x, y, name, linestyle='dotted', marker='X', markevery=1, color='blue')
    elif name == 'Full':
        return PlotOption(x, y, name, linestyle='solid', marker='^', markevery=1, color='dimgray')



def plot_flush():
    sheet = workbook.sheet_by_name("single-flush")
    fig, ax = plt.subplots(1, 1, figsize=(2.75, 2.25))
    pos = 1
    options = []
    i = 1
    for name in names:
        values = np.array(sheet.col_values(i, pos, pos + num_exprs)) / 1000
        options.append(get_option(params, values, name))
        i += 1
    lines = plot_axis(ax, None, write_memory_values, 60, options)
    ax.legend(lines, labels=names, ncol=2, loc='upper left')
    fig.tight_layout(pad=0, w_pad=0)
    path = output_path + "expr-flush-choice.pdf"
    plt.savefig(path)
    print('plotted ' + path)

plot_flush()
