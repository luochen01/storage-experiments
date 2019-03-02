
import numpy as np
import pandas
import matplotlib as mt
import matplotlib.pyplot as plt
from base import *
import itertools
from pathlib import PurePath

upsert_base_path = base_path + 'upsert/'

print(upsert_base_path)

index_upsert_antimatters = [open_csv(upsert_base_path + 'upsert-antimatter-index-1.log'), open_csv(upsert_base_path + 'upsert-antimatter-index-2.log'), open_csv(upsert_base_path + 'upsert-antimatter-index-3.log'),
                      open_csv(upsert_base_path + 'upsert-antimatter-index-4.log'), open_csv(upsert_base_path + 'upsert-antimatter-index-5.log')]
index_upsert_antimatters_totals = get_totals(index_upsert_antimatters)

index_upsert_validation_norepairs = [open_csv(upsert_base_path + 'upsert-validation-norepair-index-1.log'), open_csv(upsert_base_path + 'upsert-validation-norepair-index-2.log'), open_csv(upsert_base_path + 'upsert-validation-norepair-index-3.log'), open_csv(upsert_base_path + 'upsert-validation-norepair-index-4.log'), open_csv(upsert_base_path + 'upsert-validation-norepair-index-5.log')]
index_upsert_validation_norepairs_totals = get_totals(index_upsert_validation_norepairs)

index_upsert_validations = [open_csv(upsert_base_path + 'upsert-validation-index-1.log'), open_csv(upsert_base_path + 'upsert-validation-index-2.log'), open_csv(upsert_base_path + 'upsert-validation-index-3.log'), open_csv(upsert_base_path + 'upsert-validation-index-4.log'), open_csv(upsert_base_path + 'upsert-validation-index-5.log')]
index_upsert_validations_totals = get_totals(index_upsert_validations)

index_upsert_delete_btrees = [open_csv(upsert_base_path + 'upsert-deletebtree-index-1.log'), open_csv(upsert_base_path + 'upsert-deletebtree-index-2.log'), open_csv(upsert_base_path + 'upsert-deletebtree-index-3.log'), open_csv(upsert_base_path + 'upsert-deletebtree-index-4.log'), open_csv(upsert_base_path + 'upsert-deletebtree-index-5.log')]
index_upsert_delete_btrees_totals = get_totals(index_upsert_delete_btrees)

indexes = [1, 2, 3, 4, 5]

merge_upsert_antimatters = [open_csv(upsert_base_path + 'upsert-antimatter-merge-1073741824.log'), open_csv(upsert_base_path + 'upsert-antimatter-merge-4294967296.log'), open_csv(upsert_base_path + 'upsert-antimatter-merge-17179869184.log'), open_csv(upsert_base_path + 'upsert-antimatter-merge-68719476736.log')]
merge_upsert_antimatters_totals = get_totals(merge_upsert_antimatters)

merge_upsert_validation_norepairs = [open_csv(upsert_base_path + 'upsert-validation-norepair-merge-1073741824.log'), open_csv(upsert_base_path + 'upsert-validation-norepair-merge-4294967296.log'), open_csv(upsert_base_path + 'upsert-validation-norepair-merge-17179869184.log'), open_csv(upsert_base_path + 'upsert-validation-norepair-merge-68719476736.log')]
merge_upsert_validation_norepairs_totals = get_totals(merge_upsert_validation_norepairs)

merge_upsert_validations = [open_csv(upsert_base_path + 'upsert-validation-merge-1073741824.log'), open_csv(upsert_base_path + 'upsert-validation-merge-4294967296.log'), open_csv(upsert_base_path + 'upsert-validation-merge-17179869184.log'), open_csv(upsert_base_path + 'upsert-validation-merge-68719476736.log')]
merge_upsert_validations_totals = get_totals(merge_upsert_validations)

merge_upsert_inplaces = [open_csv(upsert_base_path + 'upsert-inplace-merge-1073741824.log'), open_csv(upsert_base_path + 'upsert-inplace-merge-4294967296.log'), open_csv(upsert_base_path + 'upsert-inplace-merge-17179869184.log'), open_csv(upsert_base_path + 'upsert-inplace-merge-68719476736.log')]
merge_upsert_inplaces_totals = get_totals(merge_upsert_inplaces)

merges = ['1GB', '4GB', '16GB', '64GB']


def plot_options(options, xvalues, ax, xlabel, ylimit):
    lines = []
    for option in options:
        line, = ax.plot(np.arange(len(xvalues)), option.data, label=option.legend, color=option.color, linestyle=option.linestyle,
                  markerfacecolor='none', markeredgecolor=option.color, marker=option.marker, markevery=1,
                  linewidth=1.0)
        lines.append(line)
    ax.set_xlabel(xlabel)
    ax.set_xlim([-0.2, len(xvalues) - 0.8])
    ax.set_xticks(np.arange(len(xvalues)))
    ax.set_xticklabels(xvalues)
    ax.set_yticklabels(['0','0.5','1','1.5'])
    ax.set_ylim(0, ylimit)
    #ax.set_yticklabels([])
    #ax.legend(handles=lines, loc='upper left', ncol=2, columnspacing=0, bbox_to_anchor=(-0.05, 1), shadow=False, framealpha=0, handlelength=0.8)

    # ax.legend(loc=2, ncol=1, handlelength=1, bbox_to_anchor=(-0.03, 1.05), labelspacing=0.08)

    return lines


def plot_shared_ingestion(options_1, options_2, xvalues_1, xvalues_2, output, xlabels,ylabel=ingestion_ylabel, ylimit=180):
    # use as global
    # set_large_fonts(shared_font_size)
    f, (ax1, ax2) = plt.subplots(1, 2, sharey=True, figsize=(5, 2))
    plt.subplots_adjust(wspace=0.05, hspace=0)
    lines1 = plot_options(options_1, xvalues_1, ax1, xlabels[0], ylimit)
    lines2 = plot_options(options_2, xvalues_2, ax2, xlabels[1], ylimit)
    
    
    legend_col = 1
    f.legend(handles=[lines1[0],lines1[1], lines1[2], lines2[-1],lines1[3]], loc='upper left', ncol=3, columnspacing=0.1, handlelength=1.0, shadow=False, framealpha=0, bbox_to_anchor=(0.05, 1.22), labelspacing=0)
    ax1.set_ylabel(ylabel)

    plt.savefig(output)
    print('output figure to ' + output)


index_options = [PlotOption(index_upsert_antimatters_totals, 'eager', marker=markers[0], linestyle=antimatter_linestyle, color=antimatter_color),
            PlotOption(index_upsert_validations_totals, 'validation', marker=markers[2], linestyle=validation_linestyle, color=validation_color),
            PlotOption(index_upsert_validation_norepairs_totals, 'validation (no repair)', marker=markers[1], linestyle=validation_norepair_linestyle, color=validation_norepair_color),
            PlotOption(index_upsert_delete_btrees_totals, 'deleted-key B+tree', marker=markers[4], linestyle=delete_btree_linestyle, color=delete_btree_color)]

merge_options = [PlotOption(merge_upsert_antimatters_totals, 'eager', marker=markers[0], linestyle=antimatter_linestyle, color=antimatter_color),
            PlotOption(merge_upsert_validations_totals, 'validation', marker=markers[2], linestyle=validation_linestyle, color=validation_color),
            PlotOption(merge_upsert_validation_norepairs_totals, 'validation (no repair)', marker=markers[1], linestyle=validation_norepair_linestyle, color=validation_norepair_color),
            PlotOption(merge_upsert_inplaces_totals, 'mutable-bitmap', marker=markers[3], linestyle=inplace_linestyle, color=inplace_color)]

# plot_shared_ingestion(options[0], options[1], options[2], ['1 Index', '3 Indexes', '5 Indexes'], result_base_path + 'upsert-secondary-index.pdf', ylimit=180)

plot_shared_ingestion(merge_options, index_options, merges, indexes, result_base_path + 'upsert-secondary-merge-index.pdf', ['(a) MaxMergeableComponentSize', '(b) Secondary Indexes'])
