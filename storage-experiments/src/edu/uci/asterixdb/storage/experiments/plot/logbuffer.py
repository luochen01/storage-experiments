
import numpy as np
import pandas
import matplotlib.pyplot as plt
import os

time_index = 'counter'
total_records_index = 'totalRecords'
ingested_records_index='ingestedRecords'
unflushed_bytes_index = 'unflushedLogBytes'
log_flush_speed_index = 'flushedLogBytes'

base_path='/Users/luochen/Documents/Research/experiments/results/logbuffer/'
result_base_path = base_path
if not os.path.exists(result_base_path):
    os.makedirs(result_base_path)

params = {
    'axes.titlesize': 16,
   'axes.labelsize': 16,
   'legend.fontsize': 16,
   'xtick.labelsize': 16,
   'ytick.labelsize': 16,
   'lines.linewidth':1.5,
   'lines.markeredgewidth':1.5,
   'text.usetex': False
}
markers=['D','s','o','*','^']

plt.rcParams.update(params)

class ExperimentResult(object):
    def __init__(self, csv):
        self.time = csv[time_index] / 1000
        self.total_records = csv[total_records_index]/1000000
        self.ingested_records = csv[ingested_records_index]
        self.unflushed_bytes = csv[unflushed_bytes_index]/1024/1024
        self.log_flush_speed = csv[log_flush_speed_index]/1024/1024

class PlotOption(object):
    def __init__(self, counter, data, legend, color='red', linestyle='solid', marker=None):
        self.counter = counter
        self.data = data
        self.linestyle = linestyle
        self.marker = marker
        self.color = color
        self.legend = legend

def open_csv(path):
    try:
        csv = pandas.read_csv(path, sep='\t')
        return ExperimentResult(csv)
    except:
        print('fail to parse '+path)
        return None


def plot_basic(options, output, title, xlabel='Time (Minutes)', ylabel='Total Ingested Records (millions)', ylimit=170):
    # use as global
    plt.tight_layout()
    plt.figure(figsize=(10, 6))
    for option in options:
        plt.plot(option.counter, option.data, label=option.legend, color=option.color, linestyle=option.linestyle,
                  markerfacecolor='none', markeredgecolor=option.color, marker=option.marker, markevery=60)

    legend_col = 1
    if len(options) > 5:
        legend_col = 2
    plt.legend(loc=2, ncol=legend_col)

    #plt.title(title)
  #  plt.xticks(np.arange(0, 70, 10))

    plt.xlim(0, 310)
    plt.ylim(0, ylimit)
    plt.gca().yaxis.grid(linestyle='dotted')
    plt.xlabel(xlabel)
    plt.ylabel(ylabel)
   # plt.show()
    plt.savefig(output)
    print('output figure to '+output)


def plot_bar(xvalues, options, output, title, xlabel='Time (Minutes)', ylabel='Total Ingested Records (millions)', ylimit=170):
    # use as global
    plt.figure(figsize=(10, 6))
    x = np.arange(len(xvalues))
    for option in options:
        plt.bar(x, option.data, align='center', label=option.legend, color=option.color, width=0.5)

    legend_col = 1
    if len(options) > 5:
        legend_col = 2
    plt.legend(loc=2, ncol=legend_col)

    #plt.title(title)
    plt.xticks(x, xvalues)

    #plt.xlim(0, 310)
    plt.ylim(0, ylimit)
    plt.gca().yaxis.grid(linestyle='dotted')
   # plt.xtick_labels(xvalues)
    plt.xlabel(xlabel)
    plt.ylabel(ylabel)
   # plt.show()
    plt.savefig(output)
    print('output figure to '+output)



log_page_size_confs = [64, 128, 256, 512, 1024, 2048, 4096, 8192]
colors = ['b', 'g', 'r', 'c', 'm', 'y', 'k', '0.75']
log_page_sizes = []
pure_log_page_sizes = []
for size in log_page_size_confs:
    log_page_sizes.append(open_csv(base_path+'buffer-'+str(size)))
    pure_log_page_sizes.append(open_csv(base_path+'buffer-pure-'+str(size)))

log_total_sizes = []
log_total_size_confs = [1024, 2048, 4096, 8192, 16384, 32768]
for size in log_total_size_confs:
    log_total_sizes.append(open_csv(base_path+'buffer-total-'+str(size)))

page_size_options = []
for i in range(0, len(log_page_size_confs)):
    page_size_options.append(PlotOption(log_page_sizes[i].time,log_page_sizes[i].total_records,'page-size-'+str(log_page_size_confs[i])+'KB', color=colors[i]) )

plot_basic(page_size_options, result_base_path+'page_size.pdf', '', 'Time (s)', 'Total Ingested Records(Million)', 9)

page_size_flush_options = []
for i in range(0, len(log_page_size_confs)):
    page_size_flush_options.append(PlotOption(log_page_sizes[i].time,log_page_sizes[i].log_flush_speed*2,'page-size-'+str(log_page_size_confs[i])+'KB', color=colors[i]) )

plot_basic(page_size_flush_options, result_base_path+'page_size_flush_speed.pdf', '', 'Time (s)', 'Flushed Log Bytes (MB)', 50)

log_total_options = []
for i in range(0, len(log_total_size_confs)):
    log_total_options.append(PlotOption(log_total_sizes[i].time,log_total_sizes[i].total_records,'log-buffer-'+str(log_total_size_confs[i])+'KB', color=colors[i]) )

plot_basic(log_total_options, result_base_path+'log_total.pdf', '', 'Time (s)', 'Total Ingested Recordss (Million)', 4)

page_size_purge_options = []
for i in range(4, len(log_page_size_confs)):
    page_size_purge_options.append(PlotOption(log_page_sizes[i].time,log_page_sizes[i].total_records,'page-size-'+str(log_page_size_confs[i])+'KB', color=colors[i]) )

page_size_purge_options.append(PlotOption(pure_log_page_sizes[7].time,pure_log_page_sizes[7].total_records,'no-disk-logging', color=colors[0]) )

plot_basic(page_size_purge_options, result_base_path+'page_size_with_pure.pdf', '', 'Time (s)', 'Total Ingested Recordss (Million)', 9)

unflushed_log_bytes_32M_option = PlotOption(log_total_sizes[5].time, log_total_sizes[5].unflushed_bytes, 'Page Size = 128KB, Log Buffer = 32MB', color=colors[0])
plot_basic([unflushed_log_bytes_32M_option], result_base_path+'unflushed-log-128K-32M.pdf', '', 'Time (s)', 'Used Log Buffer (MB)', 35)

unflushed_log_bytes_1M_option = PlotOption(log_total_sizes[0].time, log_total_sizes[0].unflushed_bytes, 'Page Size = 128KB, Log Buffer = 1MB', color=colors[0])
plot_basic([unflushed_log_bytes_1M_option], result_base_path+'unflushed-log-128K-1M.pdf', '', 'Time (s)', 'Used Log Buffer (MB)', 2)


unflushed_log_bytes_4M_option = PlotOption(log_total_sizes[2].time, log_total_sizes[2].unflushed_bytes, 'Page Size = 128KB, Log Buffer = 4MB', color=colors[0])
plot_basic([unflushed_log_bytes_4M_option], result_base_path+'unflushed-log-128K-4M.pdf', '', 'Time (s)', 'Used Log Buffer (MB)', 6)



unflushed_log_bytes_4M_32M_option = PlotOption(log_page_sizes[6].time, log_page_sizes[6].unflushed_bytes, 'Page Size = 4MB, Log Buffer = 32MB', color=colors[0])
plot_basic([unflushed_log_bytes_4M_32M_option], result_base_path+'unflushed-log-4M-32M.pdf', '', 'Time (s)', 'Used Log Buffer (MB)', 16)

def calculate_flush_speeds(result):
    counter = 0
    total_flushed_log = 0
    for j in range(0, len(result.time)):
      if result.log_flush_speed[j]>=0:
         counter += 1
         total_flushed_log +=  result.log_flush_speed[j]
    return 2 * total_flushed_log / counter


log_flush_speeds = []
for i in range(0, len(log_page_size_confs)):
    result = log_page_sizes[i]
    log_flush_speeds.append(calculate_flush_speeds(result))

log_flush_speeds.append(calculate_flush_speeds(pure_log_page_sizes[7]))

xvalues = list(log_page_size_confs)
xvalues.append('no-disk')
log_flush_speed_option = PlotOption(None, log_flush_speeds, '', color=colors[0])
plot_bar(xvalues, [log_flush_speed_option], result_base_path+"log_flush_speeds.pdf", '', 'Log Page Size (KB)', 'Log Flush Speed (MB/s)', 25)

