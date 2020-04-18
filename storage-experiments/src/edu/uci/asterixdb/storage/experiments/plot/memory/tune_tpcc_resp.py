import xlrd 
import matplotlib.pyplot as plt
import numpy as np
from matplotlib import gridspec
from base import *
from _tracemalloc import start

memory_4G = "total-4G"
memory_8G = "total-8G"
memory_12G = "total-12G"
memory_16G = "total-16G"
memory_20G = "total-20G"

time_values = range(0, 7201, 1800)

names = [memory_4G, memory_8G, memory_12G, memory_16G, memory_20G]

lens = [14, 14, 14, 14, 14]

ylimit = 3

markevery = slice(0, 13, 1)


def get_option(x, y, name):
    if name == memory_4G:
        return PlotOption(x, y, name, linestyle='solid', marker='D', markevery=markevery, color='green')
    elif name == memory_8G:
        return PlotOption(x, y, name, linestyle='solid', marker='o', markevery=markevery, color='red')
    elif name == memory_12G:
        return PlotOption(x, y, name, linestyle='solid', marker='^', markevery=markevery, color='blue')
    elif name == memory_16G:
        return PlotOption(x, y, name, linestyle='solid', marker='P', markevery=markevery, color='orange')
    elif name == memory_20G:
        return PlotOption(x, y, name, linestyle='solid', marker='X', markevery=markevery, color='dimgray')


def plot_tune_tpcc_change_step():
    fig, ax = plt.subplots(1, 2, figsize=(4.5, 2.3))

    sheet = tune_workbook.sheet_by_name("tpcc-tune-change-memory-ratio")
    
    start_index = 1
    options = []
    for i in range(0, len(names)):
        xvalues = sheet.col_values(i * 3, start_index, start_index + length)
        yvalues = sheet.col_values(i * 3 + 1, start_index, start_index + length)
        yvalues = np.array(yvalues) / 1024
        options.append(get_option(xvalues, yvalues, names[i]))
    lines = plot_axis(ax[0], "(a) Tuned Write Memory", time_values, ylimit, options, xlabel=xlabel_time, ylabel=ylabel_write_memory, use_raw_value=True)  
    
    ax[0].vlines(3600, 0, ylimit, linestyle='dashed')
    ax[0].text(3800, 1.25, 'workload\nchanged')
    
    start_index = 2
    options = []
    for i in range(0, len(names)):
        xvalues = sheet.col_values(i * 3, start_index, start_index + length)
        yvalues = sheet.col_values(i * 3 + 2, start_index, start_index + length)
        yvalues = np.array(yvalues)
        options.append(get_option(xvalues, yvalues, names[i]))
    lines = plot_axis(ax[1], "(b) Tuned I/O Cost", time_values, 160, options, xlabel=xlabel_time, ylabel=ylabel_transaction_cost, use_raw_value=True)  
       
    ax[1].vlines(3600, 0, 160, linestyle='dashed')
    ax[1].text(3800, 130, 'workload\nchanged')
   
    fig.tight_layout(pad=0.0)

    fig.legend(lines, labels=names, ncol=4, loc='upper center', borderpad=0, handlelength=1.0, columnspacing=0)
    plt.subplots_adjust(top=0.84)
    
    path = output_path + "expr-tune-tpcc-change-step.pdf"
    plt.savefig(path)
    print('plotted ' + path)


def plot_tune_tpcc_change():
    fig, ax = plt.subplots(1, 2, figsize=(4.5, 2.3))

    sheet = tune_workbook.sheet_by_name("tpcc-tune-change-memory")
    
    start_index = 1
    options = []
    for i in range(0, len(names)):
        xvalues = sheet.col_values(i * 3, start_index, start_index + lens[i])
        yvalues = sheet.col_values(i * 3 + 1, start_index, start_index + lens[i])
        yvalues = np.array(yvalues) / 1024
        options.append(get_option(xvalues, yvalues, names[i]))
    lines = plot_axis(ax[0], "(a) Tuned Write Memory", time_values, ylimit, options, xlabel=xlabel_time, ylabel=ylabel_write_memory, use_raw_value=True)  
    
    ax[0].vlines(3600, 0, ylimit, linestyle='dashed')
    ax[0].text(3800, 2.3, 'workload\nchanged')
    
    
    start_index = 2
    options = []
    for i in range(0, len(names)):
        xvalues = sheet.col_values(i * 3, start_index, start_index + lens[i])
        yvalues = sheet.col_values(i * 3 + 2, start_index, start_index + lens[i])
        yvalues = np.array(yvalues)
        options.append(get_option(xvalues, yvalues, names[i]))

    lines = plot_axis(ax[1], "(b) Tuned I/O Cost", time_values, 270, options, xlabel=xlabel_time, ylabel=ylabel_transaction_cost, use_raw_value=True)  
    ax[1].vlines(3600, 0, 270, linestyle='dashed')
    ax[1].text(3800, 130, 'workload\nchanged')
   
    fig.tight_layout(pad=0.0)

    fig.legend(lines, labels=names, ncol=5, loc='upper center', borderpad=0, handlelength=1.0, columnspacing=0)
    plt.subplots_adjust(top=0.84)    
    path = output_path + "expr-tune-tpcc-change.pdf"
    plt.savefig(path)
    print('plotted ' + path)


plot_tune_tpcc_change()
