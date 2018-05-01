
import numpy as np
import pandas
import matplotlib as mt
import matplotlib.pyplot as plt
from base import *
from pathlib import PurePath

upsert_base_path = base_path + 'upsert/'

print(upsert_base_path)

upsert_antimatter_1 = open_csv(upsert_base_path + 'upsert-antimatter-index-1.log')
upsert_antimatter_2 = open_csv(upsert_base_path + 'upsert-antimatter-index-2.log')
upsert_antimatter_3 = open_csv(upsert_base_path + 'upsert-antimatter-index-3.log')
upsert_antimatter_4 = open_csv(upsert_base_path + 'upsert-antimatter-index-4.log')
upsert_antimatter_5 = open_csv(upsert_base_path + 'upsert-antimatter-index-5.log')
upsert_antimatters = [upsert_antimatter_1, upsert_antimatter_2, upsert_antimatter_3, upsert_antimatter_4, upsert_antimatter_5]

upsert_validation_norepair_1 = open_csv(upsert_base_path + 'upsert-validation-norepair-index-1.log')
upsert_validation_norepair_2 = open_csv(upsert_base_path + 'upsert-validation-norepair-index-2.log')
upsert_validation_norepair_3 = open_csv(upsert_base_path + 'upsert-validation-norepair-index-3.log')
upsert_validation_norepair_4 = open_csv(upsert_base_path + 'upsert-validation-norepair-index-4.log')
upsert_validation_norepair_5 = open_csv(upsert_base_path + 'upsert-validation-norepair-index-5.log')

upsert_validation_norepairs = [upsert_validation_norepair_1, upsert_validation_norepair_2, upsert_validation_norepair_3, upsert_validation_norepair_4, upsert_validation_norepair_5]

upsert_validation_1 = open_csv(upsert_base_path + 'upsert-validation-index-1.log')
upsert_validation_2 = open_csv(upsert_base_path + 'upsert-validation-index-2.log')
upsert_validation_3 = open_csv(upsert_base_path + 'upsert-validation-index-3.log')
upsert_validation_4 = open_csv(upsert_base_path + 'upsert-validation-index-4.log')
upsert_validation_5 = open_csv(upsert_base_path + 'upsert-validation-index-5.log')

upsert_validations = [upsert_validation_1, upsert_validation_2, upsert_validation_3, upsert_validation_4, upsert_validation_5]

upsert_delete_btree_1 = open_csv(upsert_base_path + 'upsert-deletebtree-index-1.log')
upsert_delete_btree_2 = open_csv(upsert_base_path + 'upsert-deletebtree-index-2.log')
upsert_delete_btree_3 = open_csv(upsert_base_path + 'upsert-deletebtree-index-3.log')
upsert_delete_btree_4 = open_csv(upsert_base_path + 'upsert-deletebtree-index-4.log')
upsert_delete_btree_5 = open_csv(upsert_base_path + 'upsert-deletebtree-index-5.log')

upsert_delete_btrees = [upsert_delete_btree_1, upsert_delete_btree_2, upsert_delete_btree_3, upsert_delete_btree_4, upsert_delete_btree_5]

indexes = [1, 2, 3, 4, 5]

def plot_zipf_update_ratio(i):
    plot_basic([
            PlotOption(upsert_antimatters[i], 'eager', marker=markers[0], linestyle=antimatter_linestyle, color=antimatter_color),
            PlotOption(upsert_validation_norepairs[i], 'validation (no repair)', marker=markers[1], linestyle=validation_norepair_linestyle, color=validation_norepair_color),
            PlotOption(upsert_validations[i], 'validation', marker=markers[2], linestyle=validation_linestyle, color=validation_color),
            PlotOption(upsert_delete_btrees[i], 'deleted-key btree', marker=markers[3], linestyle=delete_btree_linestyle, color=delete_btree_color)],
            result_base_path + 'upsert-secondary-index-' + str(indexes[i]) + '.pdf', "Ingestion Performance with #Secondary Indexes " + str(indexes[i]))

for i in range(0, 5):
    plot_zipf_update_ratio(i)
