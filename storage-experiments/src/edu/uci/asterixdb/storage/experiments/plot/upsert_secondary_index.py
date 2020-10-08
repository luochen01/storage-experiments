
import numpy as np
import pandas
import matplotlib as mt
import matplotlib.pyplot as plt
from base import *
from pathlib import PurePath

index = hdd_index
lengths = [14400, 7200]

ysteps = [100, 100]
ylimits = [500, 400]

upsert_base_path = base_path + devices[index] + '/upsert/'

print(upsert_base_path)

upsert_antimatter_1 = open_csv(upsert_base_path + 'upsert-antimatter-index-1.log', lengths[index])
upsert_antimatter_2 = open_csv(upsert_base_path + 'upsert-antimatter-index-2.log')
upsert_antimatter_3 = open_csv(upsert_base_path + 'upsert-antimatter-index-3.log')
upsert_antimatter_4 = open_csv(upsert_base_path + 'upsert-antimatter-index-4.log')
upsert_antimatter_5 = open_csv(upsert_base_path + 'upsert-antimatter-index-5.log')
upsert_antimatters = [upsert_antimatter_1, upsert_antimatter_2, upsert_antimatter_3, upsert_antimatter_4, upsert_antimatter_5]
upsert_antimatters_totals = get_totals(upsert_antimatters)

upsert_validation_norepair_1 = open_csv(upsert_base_path + 'upsert-validation-norepair-index-1.log', lengths[index])
upsert_validation_norepair_2 = open_csv(upsert_base_path + 'upsert-validation-norepair-index-2.log')
upsert_validation_norepair_3 = open_csv(upsert_base_path + 'upsert-validation-norepair-index-3.log')
upsert_validation_norepair_4 = open_csv(upsert_base_path + 'upsert-validation-norepair-index-4.log')
upsert_validation_norepair_5 = open_csv(upsert_base_path + 'upsert-validation-norepair-index-5.log')
upsert_validation_norepairs = [upsert_validation_norepair_1, upsert_validation_norepair_2, upsert_validation_norepair_3, upsert_validation_norepair_4, upsert_validation_norepair_5]
upsert_validation_norepairs_totals = get_totals(upsert_validation_norepairs)

upsert_validation_1 = open_csv(upsert_base_path + 'upsert-validation-index-1.log', lengths[index])
upsert_validation_2 = open_csv(upsert_base_path + 'upsert-validation-index-2.log')
upsert_validation_3 = open_csv(upsert_base_path + 'upsert-validation-index-3.log')
upsert_validation_4 = open_csv(upsert_base_path + 'upsert-validation-index-4.log')
upsert_validation_5 = open_csv(upsert_base_path + 'upsert-validation-index-5.log')
upsert_validations = [upsert_validation_1, upsert_validation_2, upsert_validation_3, upsert_validation_4, upsert_validation_5]
upsert_validations_totals = get_totals(upsert_validations)

upsert_delete_btree_1 = open_csv(upsert_base_path + 'upsert-deletebtree-index-1.log', lengths[index])
upsert_delete_btree_2 = open_csv(upsert_base_path + 'upsert-deletebtree-index-2.log')
upsert_delete_btree_3 = open_csv(upsert_base_path + 'upsert-deletebtree-index-3.log')
upsert_delete_btree_4 = open_csv(upsert_base_path + 'upsert-deletebtree-index-4.log')
upsert_delete_btree_5 = open_csv(upsert_base_path + 'upsert-deletebtree-index-5.log')

upsert_delete_btrees = [upsert_delete_btree_1, upsert_delete_btree_2, upsert_delete_btree_3, upsert_delete_btree_4, upsert_delete_btree_5]
upsert_delete_btrees_totals = get_totals(upsert_delete_btrees)

indexes = [1, 2, 3, 4, 5]


def plot_options(options, ax, title, xlabel, xlimit, ylimit):
    lines = []
    for option in options:
        line, = ax.plot(option.data.time, option.data.total_records, label=option.legend, color=option.color, linestyle=option.linestyle,
                  markerfacecolor='none', markeredgecolor=option.color, marker=option.marker, markevery=60,
                  linewidth=1.0)
        lines.append(line)
    ax.set_title(title)
    ax.set_xlabel(xlabel)
    ax.set_xticks(np.arange(0, xlimit, step=60))
    ax.set_xlim(0, xlimit)
    ax.set_ylim(0, ylimit)
    return lines


options = []

for i in [0, 2, 4]:
    options.append([
            PlotOption(upsert_antimatters[i], 'eager', marker=markers[0], linestyle=antimatter_linestyle, color=antimatter_color),
            PlotOption(upsert_validations[i], 'validation', marker=markers[2], linestyle=validation_linestyle, color=validation_color),
            PlotOption(upsert_validation_norepairs[i], 'validation (no repair)', marker=markers[1], linestyle=validation_norepair_linestyle, color=validation_norepair_color),
            PlotOption(upsert_delete_btrees[i], 'deleted-key B+tree', marker=markers[4], linestyle=delete_btree_linestyle, color=delete_btree_color)])

xvalues = [1, 2, 3, 4, 5]
plot_totals([PlotOption(upsert_antimatters_totals, 'eager', marker=markers[0], linestyle=antimatter_linestyle, color=antimatter_color),
            PlotOption(upsert_validations_totals, 'validation', marker=markers[2], linestyle=validation_linestyle, color=validation_color),
            PlotOption(upsert_validation_norepairs_totals, 'validation (no repair)', marker=markers[1], linestyle=validation_norepair_linestyle, color=validation_norepair_color),
            PlotOption(upsert_delete_btrees_totals, 'deleted-key B+tree', marker=markers[4], linestyle=delete_btree_linestyle, color=delete_btree_color)]
            , xvalues, result_base_path + devices[index] + '-upsert-secondary-index.pdf', 'Number of Secondary Indexes', ylimit=ylimits[index])

