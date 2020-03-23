
import numpy as np
import pandas
import matplotlib.pyplot as plt
import matplotlib
import os

time_index = 'counter'
total_records_index = 'total_records'
base_path = '/Users/luochen/Documents/Research/experiments/results/lsm-new/'
result_base_path = '/Users/luochen/Documents/Research/experiments/results/lsm-new/'

antimatter_color = 'red'
antimatter_linestyle = 'solid'

validation_norepair_color = 'blue'
validation_norepair_linestyle = 'solid'

validation_color = 'green'
validation_linestyle = 'solid'

delete_btree_color = '#636363'
delete_btree_linestyle = 'solid'

inplace_color = 'orange'
inplace_linestyle = 'solid'

devices = ['hdd', 'ssd']
xlimits = [250, 130]

hdd_index = 0
ssd_index = 1

updates = [0, 0.05, 0.1, 0.25, 0.5]

if not os.path.exists(result_base_path):
    os.makedirs(result_base_path)

# del matplotlib.font_manager.weight_dict['roman']
# matplotlib.font_manager._rebuild()

font_size = 13
font_weight = 100
params = {
    'font.family': 'Times New Roman',
    'font.weight': font_weight,
    'axes.labelweight': font_weight,
    'figure.titleweight': font_weight,
    'axes.titlesize': font_size,
   'axes.labelsize': font_size,
   'legend.fontsize': font_size,
   'xtick.labelsize': font_size,
   'ytick.labelsize': font_size,
   'font.size': font_size,
   'lines.linewidth':1,
   'lines.markeredgewidth': 1,
   'lines.markersize':6,
   "legend.handletextpad":0.2,
   "legend.handlelength":1.5,
   'text.usetex': False,
   'savefig.bbox':'tight',
   'savefig.pad_inches':0,
   'figure.figsize':(3.75, 2.5),
   "legend.fancybox":True,
   "legend.shadow":False,
   "legend.framealpha":0,
   "legend.labelspacing":0.2,
   "legend.borderpad":0,
   "hatch.color":'white',
   "hatch.linewidth":'0.5',
    "xtick.direction": 'out',
    "ytick.direction": 'out',
}
markers = ['D', 's', 'o', '^', '*']
markers = ['o', '^', 'v', 'x', '*']

plt.rcParams.update(params)

# shared_fig_size = (10, 3)
# shared_font_size = 12
ingestion_xlabel = 'Time (Minutes)'
ingestion_ylabel = 'Total Records (Millions)'


def set_large_fonts(size):
    params = {
         'axes.titlesize': size,
         'axes.labelsize': size,
         'legend.fontsize': size,
         'xtick.labelsize': size,
         'ytick.labelsize': size,
    }
    print('using large fonts...')
    plt.rcParams.update(params)


class IngestionResult(object):

    def __init__(self, csv):
        self.time = csv[time_index] / 60
        self.total_records = csv[total_records_index] / 1000000


class PlotOption(object):

    def __init__(self, data, legend, color='red', linestyle='solid', marker=None, markevery=1800, alpha=None, hatch=None):
        self.data = data
        self.linestyle = linestyle
        self.marker = marker
        self.color = color
        self.legend = legend
        self.markevery = markevery
        self.alpha = alpha
        self.hatch = hatch


def open_csv(path):
    try:
        csv = pandas.read_csv(path, sep=',', header=8)
        return IngestionResult(csv)
    except:
        print('fail to parse ' + path)
        return None


def plot_basic(options, output, title, xlabel=ingestion_xlabel, ylabel=ingestion_ylabel, xlimit=365, ylimit=None, framealpha=0, ystep=50):
    # use as global
    plt.figure()
    for option in options:
        plt.plot(option.data.time, option.data.total_records, label=option.legend, color=option.color, linestyle=option.linestyle,
                  markerfacecolor='none', markeredgecolor=option.color, marker=option.marker, markevery=option.markevery)

    legend_col = 1
    if len(options) > 5:
        legend_col = 2
    plt.legend(loc=2, ncol=legend_col, framealpha=framealpha, numpoints=1)

    # plt.title(title)

    step = 0
    if xlimit / 6 <= 65:
        step = 30
    else:
        step = 300

    plt.xlabel(xlabel)
    plt.xticks(np.arange(0, xlimit, step=step))
    plt.xlim(0, xlimit)
    if ylimit != None:
        plt.ylim(0, ylimit)
        plt.yticks(np.arange(0, ylimit, step=ystep))
    else:
        plt.ylim(0)
    plt.ylabel(ylabel)

    plt.savefig(output)
    print('output figure to ' + output)


def plot_query(options, output, title, xlabel='Query', ylabel='Time (s)'):
    plt.figure()
    xvalues = []
    for option in options:
        xvalues = np.arange(len(option.data))
        plt.plot(xvalues, option.data, label=option.legend, color=option.color, linestyle=option.linestyle,
                  markerfacecolor='none', markeredgecolor=option.color, marker=option.marker, markevery=option.markevery)

    legend_col = 1
    plt.legend(loc=2, ncol=legend_col)
 #   plt.title(title)

    plt.xlabel(xlabel)
    plt.ylim(ymin=0)
    plt.ylabel(ylabel)
    plt.gca().yaxis.grid(linestyle='dotted')
    plt.savefig(output)
    print('output figure to ' + output)


def toStd(results):
    stds = []
    for result in results:
        stds.append(result.std)
    return stds


def get_totals(results):
    totals = []
    for result in results:
        tail = result.total_records.tail(1)
        totals.append(tail.sum())
    return totals


def plot_totals(options, xvalues, output, xlabel=ingestion_xlabel, ylabel=ingestion_ylabel, ylimit=240, ystep=50):
    plt.figure()
    for option in options:
        plt.plot(np.arange(len(xvalues)), option.data, label=option.legend, color=option.color, linestyle=option.linestyle,
                  markerfacecolor='none', markeredgecolor=option.color, marker=option.marker, markevery=1)
    plt.legend(loc=1, ncol=2, bbox_to_anchor=(1.04, 1), columnspacing=0.2, numpoints=1, framealpha=0, handlelength=1.3)
    plt.grid(False)
    # plt.title(title)
    plt.xlabel(xlabel)
    plt.xlim([-0.1, len(xvalues) - 0.9])
    plt.xticks(np.arange(len(xvalues)), xvalues)
    plt.ylim(0, ylimit)
    plt.ylabel(ylabel)
    
    plt.savefig(output)
    
    print('output figure to ' + output)

