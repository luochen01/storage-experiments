import xlrd 
import matplotlib.pyplot as plt
import numpy as np
from matplotlib import gridspec
from base import *

names = [btree_static_default, btree_static_tuned,
         btree_dynamic_max_memory, btree_dynamic_min_lsn, btree_dynamic_opt,
         partitioned_max_memory, partitioned_min_lsn, partitioned_opt]

field_values = ['1', '2', '3', '4', '5']

ylimit = 35


def plot_multi_sk():
    sheet = workbook.sheet_by_name("multi-sk")
    fig, axs = plt.subplots(1, 3, figsize=(8.5, 2.75))
    
    lines = []
    
    options = []
    for i in range(0, len(names)):
        values = np.array(sheet.col_values(i + 1, 8, 15)) / 1000
        options.append(get_option(params, values, names[i]))
    lines = plot_axis(axs[0], '(a) Vary Write Memory', write_memory_values, ylimit, options, xlabel=xlabel_memory)  
    
    options = []
    for i in range(0, len(names)):
        values = np.array(sheet.col_values(i + 1, 1, 6)) / 1000
        options.append(get_option(params, values, names[i]))
    lines = plot_axis(axs[1], '(b) Vary Skewness', skew_values, ylimit, options, xlabel=xlabel_skewness) 
    
    options = []
    for i in range(0, len(names)):
        values = np.array(sheet.col_values(i + 1, 17, 22)) / 1000
        options.append(get_option(params, values, names[i]))
    lines = plot_axis(axs[2], '(c) Vary Updated Fields', field_values, ylimit, options, xlabel="Number of Fields")  

    fig.tight_layout(pad=0.0, w_pad=0.1)
    fig.legend(lines, labels=names, ncol=4, loc='upper center', columnspacing=0.5, borderpad=0)
    plt.subplots_adjust(top=0.8)

    path = output_path + "expr-multi-sk.pdf"
    plt.savefig(path)
    print('plotted ' + path)


plot_multi_sk()
