
import numpy as np
import pandas
import matplotlib as mt
import matplotlib.pyplot as plt
from base import *
from pathlib import PurePath

upsert_base_path = base_path + 'dataset/'

print(upsert_base_path)

upsert_antimatter_0 = open_csv(upsert_base_path + 'upsert-antimatter-UNIFORM-0.log')
upsert_antimatter_05 = open_csv(upsert_base_path + 'upsert-antimatter-UNIFORM-0.5.log')
upsert_antimatters = [upsert_antimatter_0, upsert_antimatter_05]

upsert_validation_norepair_0 = open_csv(upsert_base_path + 'upsert-validation-norepair-UNIFORM-0.log')
upsert_validation_norepair_05 = open_csv(upsert_base_path + 'upsert-validation-norepair-UNIFORM-0.5.log')

upsert_validation_norepairs = [upsert_validation_norepair_0, upsert_validation_norepair_05]

upsert_validation_0 = open_csv(upsert_base_path + 'upsert-validation-UNIFORM-0.log')
upsert_validation_05 = open_csv(upsert_base_path + 'upsert-validation-UNIFORM-0.5.log')

upsert_validations = [upsert_validation_0, upsert_validation_05]

updates = [0, 0.5]

i = 0
plot_basic([ PlotOption(upsert_antimatters[i], 'eager', marker=markers[0], linestyle=antimatter_linestyle, color=antimatter_color, markevery=600),
        PlotOption(upsert_validation_norepairs[i], 'validation (no repair)', marker=markers[1], linestyle=validation_norepair_linestyle, color=validation_norepair_color),
        PlotOption(upsert_validations[i], 'validation', marker=markers[2], linestyle=validation_linestyle, color=validation_color)],
        result_base_path + 'upsert-dataset-' + str(updates[i]) + '.pdf', "Ingestion Performance with Update Ratio " + str(updates[i]),
        xlimit=2000, ylimit = 100)

i = 1
plot_basic([ PlotOption(upsert_antimatters[i], 'eager', marker=markers[0], linestyle=antimatter_linestyle, color=antimatter_color, markevery=600),
        PlotOption(upsert_validation_norepairs[i], 'validation (no repair)', marker=markers[1], linestyle=validation_norepair_linestyle, color=validation_norepair_color),
        PlotOption(upsert_validations[i], 'validation', marker=markers[2], linestyle=validation_linestyle, color=validation_color)],
        result_base_path + 'upsert-dataset-' + str(updates[i]) + '.pdf', "Ingestion Performance with Update Ratio " + str(updates[i]),
        xlimit=5000, ylimit = 100)
