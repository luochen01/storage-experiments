
import numpy as np
import pandas
import matplotlib.pyplot as plt
import os

time_index = 'counter'
total_records_index = 'total_records'
base_path = '/Users/luochen/Documents/Research/experiments/results/lsm/'
result_base_path = '/Users/luochen/Documents/Research/papers/lsm-paper/expr/'

antimatter_color = 'red'
antimatter_linestyle = 'solid'

validation_norepair_color = 'blue'
validation_norepair_linestyle = 'solid'

validation_color = 'green'
validation_linestyle = 'solid'

delete_btree_color = 'grey'
delete_btree_linestyle = 'solid'

inplace_color = 'orange'
inplace_linestyle = 'solid'

updates = [0, 0.05, 0.1, 0.25, 0.5]

if not os.path.exists(result_base_path):
    os.makedirs(result_base_path)

params = {
    'axes.titlesize': 11,
   'axes.labelsize': 11,
   'legend.fontsize': 11,
   'xtick.labelsize': 11,
   'ytick.labelsize': 11,
   'lines.linewidth':1,
   'lines.markeredgewidth':1,
   'text.usetex': False,
   'savefig.bbox':'tight',
   'figure.figsize':(4, 3),
   "legend.fancybox":True,
   "legend.shadow":False,
   "legend.framealpha":0
}
markers = ['D', 's', 'o', '^', '*']

plt.rcParams.update(params)


class IngestionResult(object):

    def __init__(self, csv):
        self.time = csv[time_index] / 60
        self.total_records = csv[total_records_index] / 1000000


class PlotOption(object):

    def __init__(self, data, legend, color='red', linestyle='solid', marker=None, markevery=60, alpha=None):
        self.data = data
        self.linestyle = linestyle
        self.marker = marker
        self.color = color
        self.legend = legend
        self.markevery = markevery
        self.alpha= alpha


def open_csv(path):
    try:
        csv = pandas.read_csv(path, sep=',', header=8)
        return IngestionResult(csv)
    except:
        print('fail to parse ' + path)
        return None


def plot_basic(options, output, title, xlabel='Time (Minutes)', ylabel='Total Ingested Records (millions)', xlimit=365, ylimit=170):
    # use as global

    plt.figure()
    for option in options:
        plt.plot(option.data.time, option.data.total_records, label=option.legend, color=option.color, linestyle=option.linestyle,
                  markerfacecolor='none', markeredgecolor=option.color, marker=option.marker, markevery=option.markevery,
                  linewidth=1.0)

    legend_col = 1
    if len(options) > 5:
        legend_col = 2
    plt.legend(loc=2, ncol=legend_col)

    #plt.title(title)

    step = 0
    if xlimit / 6 <= 65:
        step = 60
    else:
        step = 600

    plt.xlabel(xlabel)
    plt.xticks(np.arange(0, xlimit, step=step))
    plt.xlim(0, xlimit)
    plt.ylim(0, ylimit)
    plt.ylabel(ylabel)
    plt.gca().yaxis.grid(linestyle='dotted')
    plt.savefig(output)
    print('output figure to ' + output)


def plot_query(options, output, title, xlabel='Query', ylabel='Time (s)'):
    plt.figure()
    xvalues = []
    for option in options:
        xvalues = np.arange(len(option.data))
        plt.plot(xvalues, option.data, label=option.legend, color=option.color, linestyle=option.linestyle,
                  markerfacecolor='none', markeredgecolor=option.color, marker=option.marker, markevery=option.markevery,
                  linewidth=1.0)

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

