
import numpy as np
import pandas
import matplotlib as mt
import matplotlib.pyplot as plt
from base import *
from pathlib import PurePath

index = hdd_index

ysteps = [100, 100]
ylimits = [550, 350]

upsert_base_path = base_path + devices[index] + '/upsert/'

print(upsert_base_path)

upsert_antimatter_0 = open_csv(upsert_base_path + 'upsert-antimatter-UNIFORM-0.log')
upsert_antimatter_UNIFORM_05 = open_csv(upsert_base_path + 'upsert-antimatter-UNIFORM-0.5.log')
upsert_antimatter_ZIPF_05 = open_csv(upsert_base_path + 'upsert-antimatter-ZIPF-0.5.log')
upsert_antimatters = [upsert_antimatter_0, upsert_antimatter_UNIFORM_05, upsert_antimatter_ZIPF_05]

upsert_validation_norepair_0 = open_csv(upsert_base_path + 'upsert-validation-norepair-UNIFORM-0.log')
upsert_validation_norepair_UNIFORM_05 = open_csv(upsert_base_path + 'upsert-validation-norepair-UNIFORM-0.5.log')
upsert_validation_norepair_ZIPF_05 = open_csv(upsert_base_path + 'upsert-validation-norepair-ZIPF-0.5.log')

upsert_validation_norepairs = [upsert_validation_norepair_0, upsert_validation_norepair_UNIFORM_05, upsert_validation_norepair_ZIPF_05]

upsert_validation_0 = open_csv(upsert_base_path + 'upsert-validation-UNIFORM-0.log')
upsert_validation_UNIFORM_05 = open_csv(upsert_base_path + 'upsert-validation-UNIFORM-0.5.log')
upsert_validation_ZIPF_05 = open_csv(upsert_base_path + 'upsert-validation-ZIPF-0.5.log')

upsert_validations = [upsert_validation_0, upsert_validation_UNIFORM_05, upsert_validation_ZIPF_05]

upsert_inplace_0 = open_csv(upsert_base_path + 'upsert-inplace-UNIFORM-0.log')
upsert_inplace_UNIFORM_05 = open_csv(upsert_base_path + 'upsert-inplace-UNIFORM-0.5.log')
upsert_inplace_ZIPF_05 = open_csv(upsert_base_path + 'upsert-inplace-ZIPF-0.5.log')

upsert_inplaces = [upsert_inplace_0, upsert_inplace_UNIFORM_05, upsert_inplace_ZIPF_05]


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
    options.append([ PlotOption(upsert_antimatters[i], 'eager', marker=markers[0], linestyle=antimatter_linestyle, color=antimatter_color),
            PlotOption(upsert_validation_norepairs[i], 'validation (no repair)', marker=markers[1], linestyle=validation_norepair_linestyle, color=validation_norepair_color),
            PlotOption(upsert_validations[i], 'validation', marker=markers[2], linestyle=validation_linestyle, color=validation_color),
            PlotOption(upsert_inplaces[i], 'mutable-bitmap', marker=markers[3], linestyle=inplace_linestyle, color=inplace_color)])

# plot_shared_ingestion(options[0], options[1], options[2], ['No Update', '50% Uniform Updates', '50% Zipf Updates'], result_base_path + 'upsert-secondary-update.pdf', ylimit=180)

plot_basic(options[0], result_base_path + devices[index] + '-upsert-secondary-update-0.pdf', '', xlimit=xlimits[index], ylimit=ylimits[index], ystep=ysteps[index])
plot_basic(options[1], result_base_path + devices[index] + '-upsert-secondary-update-uniform-50.pdf', '', xlimit=xlimits[index], ylimit=ylimits[index], ystep=ysteps[index])
plot_basic(options[2], result_base_path + devices[index] + '-upsert-secondary-update-zipf-50.pdf', '', xlimit=xlimits[index], ylimit=ylimits[index], ystep=ysteps[index])

