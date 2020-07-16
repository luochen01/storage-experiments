import xlrd 
import matplotlib.pyplot as plt
import numpy as np
from matplotlib import gridspec
from base import *

names = [btree_static_default, btree_static_tuned, btree_dynamic_max_memory,
         btree_dynamic_min_lsn, btree_dynamic_opt, 
         partitioned_max_memory, partitioned_min_lsn, partitioned_opt]

ylimit = 55
params = {
    'legend.fontsize': 9,
    #"legend.labelspacing":0,
    #"legend.columnspacing":0,
    #"legend.handletextpad":0,
    "legend.handlelength":1.5,
    "legend.columnspacing":0.1,

}
plt.rcParams.update(params)
plt.tight_layout()



def plot_multi_lsm():
    sheet = workbook.sheet_by_name("multi-lsm")
    fig, ax = plt.subplots(figsize=(4.5, 2.5))
    gs = gridspec.GridSpec(1, 2, width_ratios=[5, 5]) 
    ax0 = plt.subplot(gs[0])
    ax1 = plt.subplot(gs[1])
    
    lines = []
    
    options = []
    for i in range(0, len(names)):
        values = np.array(sheet.col_values(i + 1, 8, 15)) / 1000
        options.append(get_option(params, values, names[i]))
    lines = plot_axis(ax0, '(a) Vary Write Memory', write_memory_values, ylimit, options, xlabel=xlabel_memory)  
    
    options = []
    for i in range(0, len(names)):
        values = np.array(sheet.col_values(i + 1, 1, 6)) / 1000
        options.append(get_option(params, values, names[i]))
    lines = plot_axis(ax1, '(b) Vary Skewness', skew_values, ylimit, options, xlabel=xlabel_skewness)  

    fig.tight_layout(pad=0.0, w_pad=0.1)
    fig.legend(lines, labels=names, ncol=4, loc='upper center', borderpad=0)
    plt.subplots_adjust(top=0.78)

    path = output_path + "expr-multi-lsm.pdf"
    plt.savefig(path)
    print('plotted ' + path)

plot_multi_lsm()
