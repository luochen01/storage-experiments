import xlrd 
import matplotlib.pyplot as plt
import numpy as np
from base import *

num_exprs = 7
names = [btree_static_default, btree_static_tuned, btree_dynamic, partitioned]

output_path = "/Users/luochen/Desktop/tmp/"

workloads = ["(a) Write-Only", "(b) Write-Heavy", "(c) Read-Heavy", "(d) Scan-Heavy"]

ylimits = [75, 75, 75, 7.5]



def plot_single():
    sheet = workbook.sheet_by_name("single-talk")
    fig, axs = plt.subplots(1, 4, figsize=(10, 2.5))
    lines = []
    for i in range(0, len(workloads)):
        pos = 1
        options = []
        for name in names:
            values = np.array(sheet.col_values(i + 1, pos, pos + num_exprs)) / 1000
            options.append(get_option(params, values, name))
            pos += num_exprs + 1
        lines = plot_axis(axs[i], workloads[i], write_memory_values, ylimits[i], options)

    fig.tight_layout(pad=0, w_pad=0.1)
    fig.legend(lines, labels=names, ncol=6, loc='upper center', borderpad=0)
    plt.subplots_adjust(top=0.84)

    path = output_path + "expr-single.pdf"
    plt.savefig(path)
    print('plotted ' + path)


plot_single()
