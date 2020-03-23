
import numpy as np
import pandas
import matplotlib.pyplot as plt
from base import *
from pathlib import PurePath

ylimits = [500, 450]

index = 1

insert_base_path = base_path + devices[index] + "/insert/"

print(insert_base_path)

insert_0 = open_csv(insert_base_path + 'insert-0.log')
insert_005 = open_csv(insert_base_path + 'insert-0.05.log')
insert_01 = open_csv(insert_base_path + 'insert-0.1.log')
insert_025 = open_csv(insert_base_path + 'insert-0.25.log')
insert_05 = open_csv(insert_base_path + 'insert-0.5.log')

insert_nopk_0 = open_csv(insert_base_path + 'insert-nopk-0.log')
insert_nopk_005 = open_csv(insert_base_path + 'insert-nopk-0.05.log')
insert_nopk_01 = open_csv(insert_base_path + 'insert-nopk-0.1.log')
insert_nopk_025 = open_csv(insert_base_path + 'insert-nopk-0.25.log')
insert_nopk_05 = open_csv(insert_base_path + 'insert-nopk-0.5.log')

insert_linestyle = 'solid'

set_large_fonts(14)
no_pk_linestyle = 'dashed'
plot_basic([ PlotOption(insert_0, 'pk-idx 0% dup', marker=markers[0], linestyle=insert_linestyle, color=antimatter_color),
            PlotOption(insert_05, 'pk-idx 50% dup', marker=markers[1], linestyle=insert_linestyle, color=antimatter_color),
            PlotOption(insert_nopk_0, 'no-pk-idx 0% dup', marker=markers[0], linestyle=no_pk_linestyle, color=validation_color),
            PlotOption(insert_nopk_05, 'no-pk-idx 50% dup', marker=markers[1], linestyle=no_pk_linestyle, color=validation_color)],
            result_base_path + devices[index] + '-insert.pdf', "Duplicate Ratio", ylimit=ylimits[index], xlimit=xlimits[index], ystep=100, framealpha=0.5)

