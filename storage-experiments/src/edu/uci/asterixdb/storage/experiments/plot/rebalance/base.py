import xlrd 
import matplotlib.pyplot as plt
import numpy as np

output_path = "/Users/luochen/Documents/Research/papers/rebalance/"

font_size = 10
font_weight = 100
label_size = 11
title_size = 11
legend_size = 11
params = {
    'font.family': 'Times New Roman',
    'font.weight': font_weight,
    'axes.labelweight': font_weight,
    'figure.titleweight': font_weight,
    'axes.titlesize': title_size,
   'axes.labelsize': label_size,
   'legend.fontsize': legend_size,
   'xtick.labelsize': label_size,
   'ytick.labelsize': label_size,
   'font.size': font_size,
   'lines.linewidth':1,
   'lines.markeredgewidth': 0,
   'lines.markersize':5,
   "legend.handletextpad":0.2,
   "legend.handlelength":1.5,
   'text.usetex': False,
   'savefig.bbox':'tight',
   'savefig.pad_inches':0,
   'figure.figsize':(3.25, 2.3),
   "legend.fancybox":True,
   "legend.shadow":False,
   "legend.framealpha":0,
   "legend.labelspacing":0.2,
   "legend.columnspacing":0.5,
   "legend.borderpad":0.2,
   "legend.borderaxespad":0,
   "hatch.color":'white',
   "hatch.linewidth":'0.5',
    "xtick.direction": 'out',
    "ytick.direction": 'out',
}
plt.rcParams.update(params)
plt.tight_layout()

names = ["hashing", "StaticHash", "DynaHash", "DynaHash-cleanup"]
colors = ['tomato', 'dodgerblue', 'darkgray', 'orange']


class PlotOption(object):

    def __init__(self, x, y, legend='', color='black', linestyle='solid', marker=None, markevery=1, alpha=None, hatch=None, dashes=None):
        self.x = x
        self.y = y
        self.linestyle = linestyle
        self.marker = marker
        self.color = color
        self.legend = legend
        self.markevery = markevery
        self.alpha = alpha
        self.hatch = hatch
        self.dashes = dashes


def get_sub_sheet(sheet, row_begin, row_end, col_begin, col_end):
    rows = []
    for i in range(row_begin, row_end):
        rows.append(sheet.row_values(i, col_begin, col_end))
    return rows


workbook = xlrd.open_workbook("/Users/luochen/Documents/Research/experiments/results/rebalance/result.xlsx")

xlabel_nodes = "Number of Nodes"
ylabel_time = "Time (Minutes)"
ylabel_time_sec = "Time (Seconds)"
