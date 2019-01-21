
import numpy as np
import pandas
import matplotlib.pyplot as plt
from base import *
from pathlib import PurePath

insert_base_path = base_path + 'insert-new/'

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

insert_seq = open_csv(insert_base_path + 'insert-seq.log')
insert_nopk_seq = open_csv(insert_base_path + 'insert-nopk-seq.log')

insert_multi_0 = open_csv(insert_base_path + 'insert-multi-0.log')
insert_multi_05 = open_csv(insert_base_path + 'insert-multi-0.5.log')

insert_nopk_multi_0 = open_csv(insert_base_path + 'insert-multi-nopk-0.log')
insert_nopk_multi_05 = open_csv(insert_base_path + 'insert-multi-nopk-0.5.log')


insert_linestyle = 'solid'

# plot_basic([ PlotOption(insert_0, 'pk-index-0', marker=markers[0], linestyle=insert_linestyle, color='green'),
#             PlotOption(insert_005, 'pk-index-0.05', marker=markers[1], linestyle=insert_linestyle,color='green'),
#             PlotOption(insert_01, 'pk-index-0.1', marker=markers[2], linestyle=insert_linestyle,color='green'),
#             PlotOption(insert_025, 'pk-index-0.25', marker=markers[3], linestyle=insert_linestyle,color='green'),
#             PlotOption(insert_05, 'pk-index-0.5', marker=markers[4], linestyle=insert_linestyle,color='green'),
#             PlotOption(insert_nopk_0, '0', marker=markers[0], linestyle=insert_linestyle, color='red'),
#             PlotOption(insert_nopk_005, '0.05', marker=markers[1], linestyle=insert_linestyle, color='red'),
#             PlotOption(insert_nopk_01, '0.1', marker=markers[2], linestyle=insert_linestyle, color='red'),
#             PlotOption(insert_nopk_025, '0.25', marker=markers[3], linestyle=insert_linestyle, color='red'),
#             PlotOption(insert_nopk_05, '0.5', marker=markers[4], linestyle=insert_linestyle, color='red')],
#             result_base_path+'insert.pdf', "Duplicate Ratio")
#
# plot_basic([ PlotOption(insert_0, 'pk-index-random', marker=markers[0], linestyle=insert_linestyle, color='green'),
#             PlotOption(insert_seq, 'pk-index-sequential', marker=markers[1], linestyle=insert_linestyle, color='green'),
#             PlotOption(insert_nopk_0, 'random', marker=markers[0], linestyle=insert_linestyle, color='red'),
#             PlotOption(insert_nopk_seq, 'sequential', marker=markers[1], linestyle=insert_linestyle, color='red')],
#             result_base_path+'insert-seq.pdf', "Sequential/Random Keys")

plt.rcParams.update({'figure.figsize':(3, 2.5)})


set_large_fonts(14)
no_pk_linestyle = 'dashed'
plot_basic([ PlotOption(insert_0, 'pk-idx 0% dup', marker=markers[0], linestyle=insert_linestyle, color=antimatter_color),
            PlotOption(insert_05, 'pk-idx 50% dup', marker=markers[1], linestyle=insert_linestyle, color=antimatter_color),
            PlotOption(insert_nopk_0, 'no-pk-idx 0% dup', marker=markers[0], linestyle=no_pk_linestyle, color=validation_color),
            PlotOption(insert_nopk_05, 'no-pk-idx 50% dup', marker=markers[1], linestyle=no_pk_linestyle, color=validation_color)],
            result_base_path + 'insert-long.pdf', "Duplicate Ratio", xlimit=730, ylimit=370, framealpha = 0.5, ystep=60, figsize = (3.5, 2.75))


plot_basic([ PlotOption(insert_multi_0, 'pk-idx 0% dup', marker=markers[0], linestyle=insert_linestyle, color=antimatter_color),
            PlotOption(insert_multi_05, 'pk-idx 50% dup', marker=markers[1], linestyle=insert_linestyle, color=antimatter_color),
            PlotOption(insert_nopk_multi_0, 'no-pk-idx 0% dup', marker=markers[0], linestyle=no_pk_linestyle, color=validation_color),
            PlotOption(insert_nopk_multi_05, 'no-pk-idx 50% dup', marker=markers[1], linestyle=no_pk_linestyle, color=validation_color)],
            result_base_path + 'insert-long-multi.pdf', "Duplicate Ratio", xlimit=730, ylimit=1100, framealpha = 0.5, ystep=200)


# plot_basic([ PlotOption(insert_0, 'pk-index-random', marker=markers[0], linestyle=insert_linestyle, color='green'),
#             PlotOption(insert_seq, 'pk-index-sequential', marker=markers[1], linestyle=insert_linestyle, color='green'),
#             PlotOption(insert_nopk_0, 'random', marker=markers[0], linestyle=insert_linestyle, color='red'),
#             PlotOption(insert_nopk_seq, 'sequential', marker=markers[1], linestyle=insert_linestyle, color='red')],
#             result_base_path + 'insert-seq.pdf', "Sequential/Random Keys", ylimit=180)
