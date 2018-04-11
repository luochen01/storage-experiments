
import numpy as np
import pandas
import matplotlib as mt
import matplotlib.pyplot as plt
from base import *
from pathlib import PurePath

upsert_base_path = base_path + 'upsert/'

print(upsert_base_path)

upsert_antimatter_0 = open_csv(upsert_base_path + 'upsert-antimatter-ZIPF-0.log')
upsert_antimatter_005 = open_csv(upsert_base_path + 'upsert-antimatter-ZIPF-0.05.log')
upsert_antimatter_01 = open_csv(upsert_base_path + 'upsert-antimatter-ZIPF-0.1.log')
upsert_antimatter_025 = open_csv(upsert_base_path + 'upsert-antimatter-ZIPF-0.25.log')
upsert_antimatter_05 = open_csv(upsert_base_path + 'upsert-antimatter-ZIPF-0.5.log')
upsert_antimatters = [upsert_antimatter_0, upsert_antimatter_005, upsert_antimatter_01, upsert_antimatter_025, upsert_antimatter_05]

upsert_validation_norepair_0 = open_csv(upsert_base_path + 'upsert-validation-norepair-ZIPF-0.log')
upsert_validation_norepair_005 = open_csv(upsert_base_path + 'upsert-validation-norepair-ZIPF-0.05.log')
upsert_validation_norepair_01 = open_csv(upsert_base_path + 'upsert-validation-norepair-ZIPF-0.1.log')
upsert_validation_norepair_025 = open_csv(upsert_base_path + 'upsert-validation-norepair-ZIPF-0.25.log')
upsert_validation_norepair_05 = open_csv(upsert_base_path + 'upsert-validation-norepair-ZIPF-0.5.log')

upsert_validation_norepairs = [upsert_validation_norepair_0, upsert_validation_norepair_005, upsert_validation_norepair_01, upsert_validation_norepair_025, upsert_validation_norepair_05]

upsert_validation_0 = open_csv(upsert_base_path + 'upsert-validation-ZIPF-0.log')
upsert_validation_005 = open_csv(upsert_base_path + 'upsert-validation-ZIPF-0.05.log')
upsert_validation_01 = open_csv(upsert_base_path + 'upsert-validation-ZIPF-0.1.log')
upsert_validation_025 = open_csv(upsert_base_path + 'upsert-validation-ZIPF-0.25.log')
upsert_validation_05 = open_csv(upsert_base_path + 'upsert-validation-ZIPF-0.5.log')

upsert_validations = [upsert_validation_0, upsert_validation_005, upsert_validation_01, upsert_validation_025, upsert_validation_05]

updates = [0, 0.05, 0.1, 0.25, 0.5]

antimatter_color = 'red'
antimatter_linestyle = 'solid'

validation_norepair_color = 'blue'
validation_norepair_linestyle = 'solid'

validation_color = 'green'
validation_linestyle = 'solid'

def plot_zipf_update_ratio(i):
    plot_basic([ PlotOption(upsert_antimatters[i], 'eager', marker=markers[0], linestyle=antimatter_linestyle, color=antimatter_color),
            PlotOption(upsert_validation_norepairs[i], 'validation(no repair)', marker=markers[1], linestyle=validation_norepair_linestyle, color=validation_norepair_color),
            PlotOption(upsert_validations[i], 'validation', marker=markers[2], linestyle=validation_linestyle, color=validation_color)],
            result_base_path + 'upsert-secondary-validation-ZIPF-' + str(updates[i]) + '.pdf', "Ingestion Performance with Update Ratio " + str(updates[i]))


for i in range(0, 5):
    plot_zipf_update_ratio(i)
