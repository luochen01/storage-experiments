import xlrd 
import matplotlib.pyplot as plt
import numpy as np
from matplotlib import gridspec
from base import *

names = [btree_static, btree_dynamic, partitioned_max_memory, partitioned_min_lsn, partitioned_opt]

params = {
    'font.family': 'Calibri',
}
plt.rcParams.update(params)

txn_limit = 4
write_limit = 70

output_path = "/Users/luochen/Desktop/tmp/"

def plot_tpcc():
    fig, axs = plt.subplots(1, 2, figsize=(6.5, 2.5))

    sheet = workbook.sheet_by_name("tpcc-2000")
    
    options = []
    for i in range(0, len(names)):
        values = np.array(sheet.col_values(i + 1, 1, 8)) / 1000
        options.append(get_option(params, values, names[i]))
    lines = plot_axis(axs[0], '(a) Throughput', write_memory_values, txn_limit, options, ylabel=ylabel_transaction)  
    
    options = []
    for i in range(0, len(names)):
        values = np.array(sheet.col_values(i + 1, 10, 17))
        options.append(get_option(params, values, names[i]))
    lines = plot_axis(axs[1], '(b) Write Cost', write_memory_values, write_limit, options, ylabel=ylabel_transaction_write)  

    fig.tight_layout(pad=0.0, w_pad=5)
    fig.legend(lines, labels=names, ncol=5, loc='upper center', borderpad=0)
    plt.subplots_adjust(top=0.84)
    
    path = output_path + "expr-tpcc.pdf"
    plt.savefig(path)
    print('plotted ' + path)


plot_tpcc()
