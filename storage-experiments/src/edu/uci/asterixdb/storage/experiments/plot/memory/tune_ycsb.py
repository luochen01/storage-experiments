import xlrd 
import matplotlib.pyplot as plt
import numpy as np
from matplotlib import gridspec
from base import *
from _tracemalloc import start

write_10 = "10% writes"
write_20 = "20% writes"
write_30 = "30% writes"
write_40 = "40% writes"
write_50 = "50% writes"

time_values = range(0, 3601, 1200)

names = [write_10, write_20, write_30, write_40, write_50]

lens = [7, 9, 12, 16, 19]

limit_4G = 2
limit_20G = 4

limit_cost = 15


def get_option(x, y, name):
    if name == write_10:
        return PlotOption(x, y, name, linestyle='solid', marker='D', markevery=1, color='green')
    elif name == write_20:
        return PlotOption(x, y, name, linestyle='solid', marker='o', markevery=1, color='red')
    elif name == write_30:
        return PlotOption(x, y, name, linestyle='solid', marker='^', markevery=1, color='blue')
    elif name == write_40:
        return PlotOption(x, y, name, linestyle='solid', marker='P', markevery=1, color='orange')
    elif name == write_50:
        return PlotOption(x, y, name, linestyle='solid', marker='X', markevery=1, color='dimgray')


def plot_tune_ycsb():
    fig, axs = plt.subplots(1, 4, figsize=(10, 2.5))

    sheet = tune_workbook.sheet_by_name("ycsb-tune-memory")
    
    start_index = 1
    options = []
    for i in range(0, len(names)):
        xvalues = sheet.col_values(i * 3, start_index, start_index + lens[i])
        yvalues = np.array(sheet.col_values(i * 3 + 1, start_index, start_index + lens[i])) / 1024
        options.append(get_option(xvalues, yvalues, names[i]))
    lines = plot_axis(axs[0], '(a) Tuned Write Memory-4G', time_values, limit_4G, options, xlabel=xlabel_time, ylabel=ylabel_write_memory, use_raw_value=True)  
    
    options = []
    for i in range(0, len(names)):
        xvalues = sheet.col_values(i * 3, start_index + 1, start_index + lens[i])
        yvalues = np.array(sheet.col_values(i * 3 + 2, start_index + 1, start_index + lens[i]))
        options.append(get_option(xvalues, yvalues, names[i]))
    lines = plot_axis(axs[1], '(b) Tuned I/O Cost-4G', time_values, limit_cost, options, xlabel=xlabel_time, ylabel=ylabel_op_write, use_raw_value=True)  
    
    start_index = 21
    options = []
    for i in range(0, len(names)):
        xvalues = sheet.col_values(i * 3, start_index, start_index + lens[i])
        yvalues = np.array(sheet.col_values(i * 3 + 1, start_index, start_index + lens[i])) / 1024
        options.append(get_option(xvalues, yvalues, names[i]))
    lines = plot_axis(axs[2], '(c) Tuned Write Memory-20G', time_values, limit_20G, options, xlabel=xlabel_time, ylabel=ylabel_write_memory, use_raw_value=True) 
    
    options = []
    for i in range(0, len(names)):
        xvalues = sheet.col_values(i * 3, start_index + 1, start_index + lens[i])
        yvalues = np.array(sheet.col_values(i * 3 + 2, start_index + 1, start_index + lens[i]))
        options.append(get_option(xvalues, yvalues, names[i]))
    lines = plot_axis(axs[3], '(d) Tuned I/O Cost-20G', time_values, limit_cost, options, xlabel=xlabel_time, ylabel=ylabel_op_write, use_raw_value=True)  
    
    plt.tight_layout(pad=0, w_pad=0.1)
    fig.legend(lines, labels=names, ncol=5, loc='upper center', borderpad=0)
    plt.subplots_adjust(top=0.84)
    
    path = output_path + "expr-tune-ycsb.pdf"
    plt.savefig(path)
    print('plotted ' + path)


plot_tune_ycsb()
