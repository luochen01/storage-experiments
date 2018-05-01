
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

upsert_validation_norepair_1 = open_csv(upsert_base_path + 'upsert-validation-norepair-merge-1073741824.log')
upsert_validation_norepair_4 = open_csv(upsert_base_path + 'upsert-validation-norepair-merge-4294967296.log')
upsert_validation_norepair_16 = open_csv(upsert_base_path + 'upsert-validation-norepair-merge-17179869184.log')
upsert_validation_norepair_64 = open_csv(upsert_base_path + 'upsert-validation-norepair-merge-68719476736.log')

upsert_validation_norepairs = [upsert_validation_norepair_1, upsert_validation_norepair_4, upsert_validation_norepair_16, upsert_validation_norepair_64]

upsert_validation_1 = open_csv(upsert_base_path + 'upsert-validation-merge-1073741824.log')
upsert_validation_4 = open_csv(upsert_base_path + 'upsert-validation-merge-4294967296.log')
upsert_validation_16 = open_csv(upsert_base_path + 'upsert-validation-merge-17179869184.log')
upsert_validation_64 = open_csv(upsert_base_path + 'upsert-validation-merge-68719476736.log')

upsert_validations = [upsert_validation_1, upsert_validation_4, upsert_validation_16, upsert_validation_64]

upsert_inplace_1 = open_csv(upsert_base_path + 'upsert-inplace-merge-1073741824.log')
upsert_inplace_4 = open_csv(upsert_base_path + 'upsert-inplace-merge-4294967296.log')
upsert_inplace_16 = open_csv(upsert_base_path + 'upsert-inplace-merge-17179869184.log')
upsert_inplace_64 = open_csv(upsert_base_path + 'upsert-inplace-merge-68719476736.log')

upsert_inplaces = [upsert_inplace_1, upsert_inplace_4, upsert_inplace_16, upsert_inplace_64]

merges = ['1GB', '4GB', '16GB', '64GB']


def plot_merge(i):
    plot_basic([
            PlotOption(upsert_antimatters[i], 'eager', marker=markers[0], linestyle=antimatter_linestyle, color=antimatter_color),
            PlotOption(upsert_validation_norepairs[i], 'validation (no repair)', marker=markers[1], linestyle=validation_norepair_linestyle, color=validation_norepair_color),
            PlotOption(upsert_validations[i], 'validation', marker=markers[2], linestyle=validation_linestyle, color=validation_color),
            PlotOption(upsert_inplaces[i], 'delete-bitmap', marker=markers[3], linestyle=inplace_linestyle, color=inplace_color)],
            result_base_path + 'upsert-secondary-merge-' + str(merges[i]) + '.pdf', "Ingestion Performance with MaxMergeableComponentSize " + str(merges[i]))


for i in range(0, 4):
    plot_merge(i)
