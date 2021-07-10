import xlrd 
import matplotlib.pyplot as plt
import numpy as np
from matplotlib import gridspec
from base import *

write_cost = "write-cost"
merge_read_cost = "merge-read-cost"
query_read_cost = "query-read-cost"
total_cost = "total-cost"

mems_2G = np.array([64, 128, 256, 512, 768, 1024, 1280, 1536, 1664, 1792]) / 1024
mems_8G = np.array([64, 128, 256, 512, 768, 1024, 1536, 2048, 3072, 4096, 6144, 7168, 7680]) / 1024

names = [write_cost, merge_read_cost, query_read_cost, total_cost]


def get_option(x, y, name):
    if name == write_cost:
        return PlotOption(x, y, name, linestyle='solid', marker='D', markevery=1, color='green')
    elif name == merge_read_cost:
        return PlotOption(x, y, name, linestyle='solid', marker='o', markevery=1, color='red')
    elif name == query_read_cost:
        return PlotOption(x, y, name, linestyle='solid', marker='^', markevery=1, color='blue')
    elif name == total_cost:
        return PlotOption(x, y, name, linestyle='solid', marker='P', markevery=1, color='orange')


def plot_tpcc():
    fig, axs = plt.subplots(1, 2, figsize=(5, 2.5))

    sheet = tune_workbook.sheet_by_name("ycsb-plot")
    
    pos = 7
    options = []
    for i in range(0, len(names)):
        costs = np.array(sheet.col_values(pos + i, 1, 14))
        option = get_option(mems_8G, costs, names[i]);
        options.append(option)
    lines = plot_axis(axs[0], '(a) YCSB (Write-Heavy)', [0, 2, 4, 6, 8], 20, options, ylabel=ylabel_op_io, use_raw_value=True)  

    sheet = tune_workbook.sheet_by_name("tpcc-plot")
    
    pos = 7
    options = []
    for i in range(0, len(names)):
        costs = np.array(sheet.col_values(pos + i, 1, 14))
        option = get_option(mems_8G, costs, names[i]);
        options.append(option)
    lines = plot_axis(axs[1], '(b) TPCC (SF=2000)', [0, 2, 4, 6, 8], 400, options, ylabel=ylabel_transaction_cost, use_raw_value=True)  
  
    fig.tight_layout(pad=0.0, w_pad=1.5)
    fig.legend(lines, labels=names, ncol=4, loc='upper center', borderpad=0)
    plt.subplots_adjust(top=0.9)
    
    path = output_path + "expr-plot-tpcc.pdf"
    plt.savefig(path)
    print('plotted ' + path)


plot_tpcc()
