
import numpy as np
import pandas
import matplotlib as mt
import matplotlib.pyplot as plt
from base import *
from pathlib import PurePath

upsert_base_path = base_path + 'upsert/'

print(upsert_base_path)

upsert_antimatter_1 = open_csv(upsert_base_path + 'upsert-antimatter-merge-1073741824.log')
upsert_antimatter_4 = open_csv(upsert_base_path + 'upsert-antimatter-merge-4294967296.log')
upsert_antimatter_16 = open_csv(upsert_base_path + 'upsert-antimatter-merge-17179869184.log')
upsert_antimatter_64 = open_csv(upsert_base_path + 'upsert-antimatter-merge-68719476736.log')
upsert_antimatters = [upsert_antimatter_1, upsert_antimatter_4, upsert_antimatter_16, upsert_antimatter_64]
upsert_antimatters_totals = get_totals(upsert_antimatters)

upsert_validation_norepair_1 = open_csv(upsert_base_path + 'upsert-validation-norepair-merge-1073741824.log')
upsert_validation_norepair_4 = open_csv(upsert_base_path + 'upsert-validation-norepair-merge-4294967296.log')
upsert_validation_norepair_16 = open_csv(upsert_base_path + 'upsert-validation-norepair-merge-17179869184.log')
upsert_validation_norepair_64 = open_csv(upsert_base_path + 'upsert-validation-norepair-merge-68719476736.log')
upsert_validation_norepairs = [upsert_validation_norepair_1, upsert_validation_norepair_4, upsert_validation_norepair_16, upsert_validation_norepair_64]
upsert_validation_norepairs_totals = get_totals(upsert_validation_norepairs)

upsert_validation_1 = open_csv(upsert_base_path + 'upsert-validation-merge-1073741824.log')
upsert_validation_4 = open_csv(upsert_base_path + 'upsert-validation-merge-4294967296.log')
upsert_validation_16 = open_csv(upsert_base_path + 'upsert-validation-merge-17179869184.log')
upsert_validation_64 = open_csv(upsert_base_path + 'upsert-validation-merge-68719476736.log')
upsert_validations = [upsert_validation_1, upsert_validation_4, upsert_validation_16, upsert_validation_64]
upsert_validations_totals = get_totals(upsert_validations)

upsert_inplace_1 = open_csv(upsert_base_path + 'upsert-inplace-merge-1073741824.log')
upsert_inplace_4 = open_csv(upsert_base_path + 'upsert-inplace-merge-4294967296.log')
upsert_inplace_16 = open_csv(upsert_base_path + 'upsert-inplace-merge-17179869184.log')
upsert_inplace_64 = open_csv(upsert_base_path + 'upsert-inplace-merge-68719476736.log')
upsert_inplaces = [upsert_inplace_1, upsert_inplace_4, upsert_inplace_16, upsert_inplace_64]
upsert_inplaces_totals = get_totals(upsert_inplaces)
merges = ['1GB', '4GB', '16GB', '64GB']


def plot_options(options, ax, title, xlabel, xlimit, ylimit):
    lines = []
    for option in options:
        line, = ax.plot(option.data.time, option.data.total_records, label=option.legend, color=option.color, linestyle=option.linestyle,
                  markerfacecolor='none', markeredgecolor=option.color, marker=option.marker, markevery=60,
                  linewidth=1.0)
        lines.append(line)
    ax.set_title(title)
    ax.set_xlabel(xlabel)
    ax.set_xticks(np.arange(0, xlimit, step=120))
    ax.set_xlim(0, xlimit)
    ax.set_ylim(0, ylimit)
    return lines


def plot_shared_ingestion(options_1, options_2, options_3, titles, output, xlabel=ingestion_xlabel, ylabel=ingestion_ylabel, xlimit=370, ylimit=1100):
    # use as global
    set_large_fonts(shared_font_size)
    f, (ax1, ax2, ax3) = plt.subplots(1, 3, sharey=True, figsize=shared_fig_size)
    plt.subplots_adjust(wspace=0.1, hspace=0)
    lines = plot_options(options_1, ax1, titles[0] , "", xlimit, ylimit)
    plot_options(options_2, ax2, titles[1], xlabel, xlimit, ylimit)
    plot_options(options_3, ax3, titles[2], "", xlimit, ylimit)

    legend_col = 1
    f.legend(handles=lines, loc='upper left', ncol=2, columnspacing=4.2, bbox_to_anchor=(0.08, 1.03), shadow=False, framealpha=0)

    # ax1.legend(loc=2, ncol=legend_col)
    ax1.set_ylabel(ylabel)

    plt.savefig(output)
    print('output figure to ' + output)


options = []

for i in [0, 1, 2]:
    options.append([
            PlotOption(upsert_antimatters[i], 'eager', marker=markers[0], linestyle=antimatter_linestyle, color=antimatter_color),
            PlotOption(upsert_validations[i], 'validation', marker=markers[2], linestyle=validation_linestyle, color=validation_color),
            PlotOption(upsert_validation_norepairs[i], 'validation (no repair)', marker=markers[1], linestyle=validation_norepair_linestyle, color=validation_norepair_color),
            PlotOption(upsert_inplaces[i], 'delete-bitmap', marker=markers[3], linestyle=inplace_linestyle, color=inplace_color)])


def plot_merge(i):
    plot_basic([
            PlotOption(upsert_antimatters[i], 'eager', marker=markers[0], linestyle=antimatter_linestyle, color=antimatter_color),
            PlotOption(upsert_validation_norepairs[i], 'validation (no repair)', marker=markers[1], linestyle=validation_norepair_linestyle, color=validation_norepair_color),
            PlotOption(upsert_validations[i], 'validation', marker=markers[2], linestyle=validation_linestyle, color=validation_color),
            result_base_path + 'upsert-secondary-merge-' + str(merges[i]) + '.pdf', "Ingestion Performance with MaxMergeableComponentSize " + str(merges[i])])


#plot_shared_ingestion(options[0], options[1], options[2], ['1GB', '4GB', '16GB'], result_base_path + 'upsert-secondary-merge.pdf', ylimit=180)


plot_totals([PlotOption(upsert_antimatters_totals, 'eager', marker=markers[0], linestyle=antimatter_linestyle, color=antimatter_color),
            PlotOption(upsert_validations_totals, 'validation', marker=markers[2], linestyle=validation_linestyle, color=validation_color),
            PlotOption(upsert_validation_norepairs_totals, 'validation (no repair)', marker=markers[1], linestyle=validation_norepair_linestyle, color=validation_norepair_color),
            PlotOption(upsert_inplaces_totals, 'mutable-bitmap', marker=markers[3], linestyle=inplace_linestyle, color=inplace_color)],
             merges, result_base_path + 'upsert-secondary-merge-total.pdf', 'Maximum Mergeable Component Size')

