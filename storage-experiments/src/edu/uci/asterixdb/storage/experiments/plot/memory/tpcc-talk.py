import xlrd 
import matplotlib.pyplot as plt
import numpy as np
from matplotlib import gridspec
from base import *

output_path = "/Users/luochen/Desktop/tmp/"


names = [btree_static, btree_dynamic_max_memory, btree_dynamic_min_lsn,btree_dynamic_opt,
          partitioned_max_memory, partitioned_min_lsn, partitioned_opt]

txn_limit = 5
write_limit = 70


def plot_tpcc():
    fig, axs = plt.subplots(1, 4, figsize=(10, 2.4))
    #axs = [tmp[0][0], tmp[0][1], tmp[1][0], tmp[1][1]]

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

    fig.tight_layout(pad=0, w_pad=2.0, h_pad=1.5)
    fig.legend(lines, labels=names, ncol=7, loc='upper center', borderpad=0)
    plt.subplots_adjust(top=0.8)
    
    path = output_path + "expr-tpcc.pdf"
    plt.savefig(path)
    print('plotted ' + path)


plot_tpcc()
