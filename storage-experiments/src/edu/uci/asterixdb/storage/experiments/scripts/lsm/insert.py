
import numpy as np
import pandas
import matplotlib.pyplot as plt
from base import *
from pathlib import PurePath

insert_base_path = base_path + 'insert-ssd-new/'

print(insert_base_path)

insert_hdd_0 = open_csv(insert_base_path + 'insert-0.log')
insert_hdd_05 = open_csv(insert_base_path + 'insert-0.5.log')

insert_hdd_nopk_0 = open_csv(insert_base_path + 'insert-nopk-0.log')
insert_hdd_nopk_05 = open_csv(insert_base_path + 'insert-nopk-0.5.log')

insert_base_path = base_path + 'insert-ssd/'

insert_ssd_0 = open_csv(insert_base_path + 'insert-0.log')
insert_ssd_05 = open_csv(insert_base_path + 'insert-0.5.log')

insert_ssd_nopk_0 = open_csv(insert_base_path + 'insert-nopk-0.log')
insert_ssd_nopk_05 = open_csv(insert_base_path + 'insert-nopk-0.5.log')


def plot_options(options, ax, title, xlabel, xlimit, ylimit,yticks, ylabels, xstep, ystep):
    lines = []
    for option in options:
        line, = ax.plot(option.data.time, option.data.total_records, label=option.legend, color=option.color, linestyle=option.linestyle,
                  markerfacecolor='none', markeredgecolor=option.color, marker=option.marker, markevery=60,
                  linewidth=1.0)
        lines.append(line)
    ax.set_title(title)
    ax.set_xlabel(xlabel)
    ax.set_xticks(np.arange(0, xlimit, step=xstep))
    ax.set_yticks(yticks)
    ax.set_yticklabels(ylabels)
    ax.set_xlim(0, xlimit)
    ax.set_ylim(0, ylimit)
    
    ax.legend(loc=2, ncol=1, handlelength=1, bbox_to_anchor=(-0.03, 1.05), labelspacing=0.08, framealpha=0.5)

    return lines


def plot_shared_ingestion(options_1, options_2, titles, output, xlimits, ylimits, yticks, ylabels, xlabel=ingestion_xlabel, ylabel=ingestion_ylabel):
    # use as global
    #set_large_fonts(shared_font_size)
    f, (ax1, ax2) = plt.subplots(1, 2, sharey=False, figsize=(5.5,2.2))
    plt.subplots_adjust(wspace=0.12, hspace=0)
    lines = plot_options(options_1, ax1, titles[0] , xlabel, xlimits[0], ylimits[0],yticks[0], ylabels[0], 240, 120)
    plot_options(options_2, ax2, titles[1], xlabel, xlimits[1], ylimits[1],yticks[1], ylabels[1], 120, 150)

    legend_col = 1
    #f.legend(handles=lines, loc='upper left', ncol=2, columnspacing=4.2, bbox_to_anchor=(0.08, 1.03), shadow=False, framealpha=0)
    #ax1.legend(loc=2, ncol=legend_col)
    ax1.set_ylabel(ylabel)
    plt.savefig(output)
    print('output figure to ' + output)

insert_linestyle = 'solid'
no_pk_linestyle = 'dashed'

hdd_options = [ PlotOption(insert_hdd_0, 'pk-idx 0% dup', marker=markers[0], linestyle=insert_linestyle, color=antimatter_color),
            PlotOption(insert_hdd_05, 'pk-idx 50% dup', marker=markers[1], linestyle=insert_linestyle, color=antimatter_color),
            PlotOption(insert_hdd_nopk_0, 'no-pk-idx 0% dup', marker=markers[0], linestyle=no_pk_linestyle, color=validation_color),
            PlotOption(insert_hdd_nopk_05, 'no-pk-idx 50% dup', marker=markers[1], linestyle=no_pk_linestyle, color=validation_color)]

ssd_options = [ PlotOption(insert_ssd_0, 'pk-idx 0% dup', marker=markers[0], linestyle=insert_linestyle, color=antimatter_color),
            PlotOption(insert_ssd_05, 'pk-idx 50% dup', marker=markers[1], linestyle=insert_linestyle, color=antimatter_color),
            PlotOption(insert_ssd_nopk_0, 'no-pk-idx 0% dup', marker=markers[0], linestyle=no_pk_linestyle, color=validation_color),
            PlotOption(insert_ssd_nopk_05, 'no-pk-idx 50% dup', marker=markers[1], linestyle=no_pk_linestyle, color=validation_color)]

plot_shared_ingestion(hdd_options, ssd_options, ['On Hard Disk', 'On SSD'], result_base_path + 'insert.pdf', [730, 365], [370, 650],
                      [[0, 100, 200, 300], [0,200, 400, 600]], [['0', '1', '2', '3'], ['0','2','4','6']])
