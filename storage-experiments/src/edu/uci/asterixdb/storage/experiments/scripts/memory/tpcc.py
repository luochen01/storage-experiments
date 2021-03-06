import xlrd 
import matplotlib.pyplot as plt
import numpy as np
from matplotlib import gridspec
from base import *

names = [btree_static, btree_dynamic_max_memory, btree_dynamic_min_lsn,btree_dynamic_opt,
          partitioned_max_memory, partitioned_min_lsn, partitioned_opt]

txn_limit = 5
write_limit = 70


def plot_tpcc():
    fig, tmp = plt.subplots(2, 2, figsize=(6,6))
    axs = [tmp[0][0], tmp[0][1], tmp[1][0], tmp[1][1]]

    sheet = workbook.sheet_by_name("tpcc-500")
    
    options = []
    for i in range(0, len(names)):
        values = sheet.col_values(i + 1, 1, 8)
        values = np.array(values) / 1000
        options.append(get_option(params, values, names[i]))
    lines = plot_axis(axs[0], '(a) Throughput (SF=500)', write_memory_values, txn_limit, options, ylabel=ylabel_transaction)  
    
    options = []
    for i in range(0, len(names)):
        values = np.array(sheet.col_values(i + 1, 10, 17))
        options.append(get_option(params, values, names[i]))
    lines = plot_axis(axs[1], '(b) Write Cost (SF=500)', write_memory_values, write_limit, options, ylabel=ylabel_transaction_write)  

    sheet = workbook.sheet_by_name("tpcc-2000")
    
    options = []
    for i in range(0, len(names)):
        values = np.array(sheet.col_values(i + 1, 1, 8)) / 1000
        options.append(get_option(params, values, names[i]))
    lines = plot_axis(axs[2], '(c) Throughput (SF=2000)', write_memory_values, txn_limit, options, ylabel=ylabel_transaction)  
    
    options = []
    for i in range(0, len(names)):
        values = np.array(sheet.col_values(i + 1, 10, 17))
        options.append(get_option(params, values, names[i]))
    lines = plot_axis(axs[3], '(d) Write Cost (SF=2000)', write_memory_values, write_limit, options, ylabel=ylabel_transaction_write)  

    fig.tight_layout(pad=0, w_pad=1.5, h_pad=1.5)
    fig.legend(lines, labels=names, ncol=4, loc='upper center', borderpad=0)
    plt.subplots_adjust(top=0.92)
    
    path = output_path + "expr-tpcc.pdf"
    plt.savefig(path)
    print('plotted ' + path)


plot_tpcc()
