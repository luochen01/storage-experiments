import xlrd 
import matplotlib.pyplot as plt
import numpy as np
from base import *

num_exprs = 7
names = [btree_static_default, btree_static_tuned, btree_dynamic, accordian_data, accordion_index, partitioned]

workloads = ["(a) Write-Only", "(b) Write-Heavy", "(c) Read-Heavy", "(d) Scan-Heavy"]

ylimits = [75, 75, 75, 7.5]


def plot_single():
    sheet = workbook.sheet_by_name("single")
    fig, tmp = plt.subplots(2, 2, figsize=(5, 5))
    axs = [tmp[0][0], tmp[0][1], tmp[1][0], tmp[1][1]]
    
    lines = []
    for i in range(0, len(workloads)):
        pos = 1
        options = []
        for name in names:
            values = np.array(sheet.col_values(i + 1, pos, pos + num_exprs)) / 1000
            options.append(get_option(params, values, name))
            pos += num_exprs + 1
        lines = plot_axis(axs[i], workloads[i], write_memory_values, ylimits[i], options)

    fig.tight_layout(pad=0, w_pad=1.5, h_pad=1.5)
    fig.legend(lines, labels=names, ncol=3, loc='upper center', borderpad=0)
    plt.subplots_adjust(top=0.92)

    path = output_path + "expr-single.pdf"
    plt.savefig(path)
    print('plotted ' + path)

plot_single()
