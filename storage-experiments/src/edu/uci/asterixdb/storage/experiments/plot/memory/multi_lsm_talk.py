import xlrd 
import matplotlib.pyplot as plt
import numpy as np
from matplotlib import gridspec
from base import *

names = [btree_static_default, btree_static_tuned, btree_dynamic, partitioned_max_memory, partitioned_min_lsn, partitioned_opt]

params = {
    'font.family': 'Calibri',
}
plt.rcParams.update(params)

ylimit = 55

output_path = "/Users/luochen/Desktop/tmp/"

def plot_multi_lsm():
    sheet = workbook.sheet_by_name("multi-lsm")
    fig, axs = plt.subplots(1, 2, figsize=(6.5, 2.8))
    
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

    fig.tight_layout(pad=0, w_pad=5)
    fig.legend(lines, labels=names, ncol=3, loc='upper center', borderpad=0)
    plt.subplots_adjust(top=0.78)

    path = output_path + "expr-multi-lsm.pdf"
    plt.savefig(path)
    print('plotted ' + path)


plot_multi_lsm()
