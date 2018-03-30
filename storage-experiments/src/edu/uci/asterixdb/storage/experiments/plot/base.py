
import numpy as np
import pandas
import matplotlib.pyplot as plt
import os

time_index = 'counter'
total_records_index = 'total_records'
base_path='/Users/luochen/Documents/Research/experiments/results/lsm/'
result_base_path='/Users/luochen/Documents/Research/experiments/results/lsm/figure/'

if not os.path.exists(result_base_path):
    os.makedirs(result_base_path)

params = {
    'axes.titlesize': 24,
   'axes.labelsize': 20,
   'legend.fontsize': 12,
   'xtick.labelsize': 16,
   'ytick.labelsize': 16,
   'lines.linewidth':1,
   'lines.markeredgewidth':1.5,
   'text.usetex': False
}
markers=['D','s','o','*','^']

#plt.rcParams.update(params)

class ExperimentResult(object):
    def __init__(self, csv):
        self.time = csv[time_index] / 60
        self.total_records = csv[total_records_index]/1000000

class PlotOption(object):
    def __init__(self, data, legend, color='red', linestyle='solid', marker=None):
        self.data = data
        self.linestyle = linestyle
        self.marker = marker
        self.color = color
        self.legend = legend

def open_csv(path):
    try:
        csv = pandas.read_csv(path, sep=',', header=8)
        return ExperimentResult(csv)
    except:
        print('fail to parse '+path)
        return None


def plot_basic(options, output, title, xlabel='Time (Minutes)', ylabel='Total Ingested Records (millions)', ylimit=170):
    # use as global

    plt.figure()
    for option in options:
        plt.plot(option.data.time, option.data.total_records, label=option.legend, color=option.color, linestyle=option.linestyle,
                  markerfacecolor='none', markeredgecolor=option.color, marker=option.marker, markevery=60,
                  linewidth=1.0)

    legend_col = 1
    if len(options) > 5:
        legend_col = 2
    plt.legend(loc=2, ncol=legend_col)

    plt.title(title)
  #  plt.xticks(np.arange(0, 70, 10))

    plt.xlabel(xlabel)
    plt.xticks(np.arange(0, 365, step=60))
    plt.xlim(0, 365)
    plt.ylim(0, ylimit)
    plt.ylabel(ylabel)
    plt.gca().yaxis.grid(linestyle='dotted')
    #plt.show()
    plt.savefig(output)
    print('output figure to '+output)
