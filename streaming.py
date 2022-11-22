import json
import os
import shutil
import sys
import datetime
import math
import tracemalloc
import time
import linecache
import threading
import requests
import matplotlib.animation as animation
import matplotlib.pyplot as plt
import numpy as np
import pyspark
from pyspark import RDD, AccumulatorParam, SparkConf, SparkContext
from pyspark.streaming import StreamingContext
from dec_analyzer import CustomizedAnalyzer, CustomizedAnalyzerBrute
from ul_analyzer import UplinkLatencyAnalyzer
from mobile_insight.monitor import OfflineReplayer
import warnings

warnings.filterwarnings("ignore")

# uses tracemalloc to determine memory allocations
def display_top(snapshot, key_type='lineno', limit=3):
    snapshot = snapshot.filter_traces((
        tracemalloc.Filter(False, "<frozen importlib._bootstrap>"),
        tracemalloc.Filter(False, "<unknown>"),
    ))
    top_stats = snapshot.statistics(key_type)

    print("Top %s lines" % limit)
    for index, stat in enumerate(top_stats[:limit], 1):
        frame = stat.traceback[0]
        # replace "/path/to/module/file.py" with "module/file.py"
        filename = os.sep.join(frame.filename.split(os.sep)[-2:])
        print("#%s: %s:%s: %.1f KiB"
              % (index, filename, frame.lineno, stat.size / 1024))
        line = linecache.getline(frame.filename, frame.lineno).strip()
        if line:
            print('    %s' % line)

    other = top_stats[limit:]
    if other:
        size = sum(stat.size for stat in other)
        print("%s other: %.1f KiB" % (len(other), size / 1024))
    total = sum(stat.size for stat in top_stats)
    print("Total allocated size: %.1f KiB" % (total / 1024))


class SetAccumulatorParam(AccumulatorParam):
    def zero(self, value = ""):
        return set()

    def addInPlace(self, value1: set(), value2: set()):
        value1.update(value2)
        return value1

# Represents an Accumulator Array that we can continually apppend new data onto as it is streamed in
class ArrayAccumulatorParam(AccumulatorParam):
    def zero(self, value = ""):
        return []

    def addInPlace(self, value1: list, value2: list):
        value1 += value2
        return value1

class WindowAccumulatorParam(AccumulatorParam):
    def zero(self, value = ""):
        return 0

    def addInPlace(self, value1, value2: list):
        sum = 0
        for entry in value2:
            sum += entry[1]

        value1 = sum
        return value1

class MaxAccumulatorParam(AccumulatorParam):
    def zero(self, value = ""):
        return 0

    def addInPlace(self, value1, value2):
        value1 = value2
        return value1

# called for each RDD in spark streaming, decodes and processes new mi2log file data and adds
# the resulting data to the sliding window of the plot
def addToWindow(rdd: RDD):
    import warnings
    warnings.filterwarnings("ignore")
    if rdd.isEmpty():
        return

    global processed_files
    global sc
    global running_resource_blocks
    global total_res_blocks_per_window
    global waiting_accum
    global tx_accum
    global retx_accum

    # determine new and visited files in target directory
    new_files = set()
    visited_files = processed_files.value

    for filename in os.listdir('/sdcard/mobileinsight/logs/'):
        file_path = os.path.join('/sdcard/mobileinsight/logs/', filename)
        try:
            file_name = file_path.split('/')[-1]
            if (os.path.isfile(file_path) or os.path.islink(file_path)) and not file_name in visited_files:
                new_files.add(file_name)
        except Exception as e:
            print('Error with file %s. Reason: %s' % (file_path, e))

    print('visited:', visited_files)
    print('new:', new_files)

    # run our customized analyzer to grab log data for channel utilization 
    src = OfflineReplayer()      
    src.set_input_path('/sdcard/mobileinsight/logs/')
    analyzer_dl = CustomizedAnalyzerBrute()
    analyzer_dl.set_source(src)
    src.run_files(new_files)
    raw_data = analyzer_dl.logs
    print('number of messages:', len(raw_data))

    processed_files += new_files

    # run uplink analyzer to grab log data for uplink latency
    src = OfflineReplayer()      
    src.set_input_path('/sdcard/mobileinsight/logs/')
    analyzer_ul = UplinkLatencyAnalyzer()
    analyzer_ul.set_source(src)
    src.run_files(new_files)
    
    # spark map operations on rdd to parse log messages and grab resource block info
    data_rdd = sc.parallelize(raw_data)
    # line_stream = data_rdd.flatMap(lambda log: log.split("\n"))
    evaluated_stream = data_rdd.map(lambda object: eval(object))
    # res_blocks_stream = evaluated_stream.flatMap(lambda log: getUniqueSequntialRBs(log))
    res_blocks_stream = evaluated_stream.flatMap(lambda log: getUniqueRBs(log))


    print("Running...")

    data = res_blocks_stream.collect()
    running_resource_blocks += data
    total_res_blocks_per_window += data
    total_rbs = total_res_blocks_per_window.value
    print('total rbs:', total_rbs)
    utilization = map(lambda entry: entry[1], data)
    min_timestamp = float('inf')
    max_timestamp = 0
    for entry in data:
        if entry[0] > max_timestamp:
            max_timestamp = math.trunc(entry[0])
        if entry[0] < min_timestamp:
            min_timestamp = math.trunc(entry[0])

    global waiting_arr
    global tx_arr
    global retx_arr
    global starttime

    # if data exists in this iterations mi2logs, generate plots
    if data:
        # https://matplotlib.org/3.1.1/api/_as_gen/matplotlib.pyplot.hist.html stacked and framed
        # https://matplotlib.org/3.1.1/gallery/subplots_axes_and_figures/demo_constrained_layout.html gridpsec? 
        # https://www.qubole.com/blog/apache-spark-use-cases/
        print("Processing...")

        # channel utilization plots
        fig, ax = plt.subplots()
        kwargs = dict(alpha=0.7, bins='auto', color='#0504aa', rwidth=0.85, density=True, stacked=True)
        n, bins, patches = plt.hist(x=list(utilization), **kwargs)
        start = datetime.datetime.fromtimestamp(min_timestamp)
        end = datetime.datetime.fromtimestamp(max_timestamp)
        duration = round((max_timestamp-min_timestamp)/60,3)
        # plt.title('Channel Utilization from {start} to {end}'.format(start=start, end=end, duration=duration))
        plt.title('Channel Utilization starting {start} (Duration: {duration} min)'.format(start=start, duration=duration))
        plt.xlabel('Number of Resource Blocks ({total_rbs} Total Resource Blocks in Time Window)'.format(total_rbs=total_rbs))
        plt.ylabel('Probability')
        plt.grid(axis='y', alpha=0.75)
        plt.ylim(ymax=0.5)
        fig.savefig('/sdcard/plots/dec{counter}_plot.png'.format(counter = png_counter.value))
        plt.close(fig)

        # UPLINK LATENCY map reduce operations and plot
        latency = analyzer_ul.all_packets
        lat_rdd = sc.parallelize(latency)

        waiting_values = lat_rdd.map(lambda element: element["Waiting Latency"])
        tx_values = lat_rdd.map(lambda element: element["Tx Latency"])
        retx_values = lat_rdd.map(lambda element: element["Retx Latency"])

        waiting_tot = waiting_values.reduce(lambda a, b: a + b)
        tx_tot = tx_values.reduce(lambda a, b: a + b)
        retx_tot = retx_values.reduce(lambda a, b: a + b)

        waiting_accum.add(waiting_tot)
        tx_accum.add(tx_tot)
        retx_accum.add(retx_tot)

        waiting_arr.append(waiting_tot)
        tx_arr.append(tx_tot)
        retx_arr.append(retx_tot)

        fig, ax = plt.subplots()
        ax.bar('Latency', waiting_tot, label = 'Waiting Latency: {}'.format(waiting_tot), width=0.2)
        ax.bar('Latency', tx_tot, bottom = waiting_tot, label = 'Tx Latency: {}'.format(tx_tot), width=0.2)
        ax.bar('Latency', retx_tot, bottom = waiting_tot + tx_tot, label = 'Retx Latency: {}'.format(retx_tot), width=0.2)
        plt.legend()
        start = datetime.datetime.fromtimestamp(min_timestamp)
        end = datetime.datetime.fromtimestamp(max_timestamp)
        duration = round((max_timestamp-min_timestamp)/60,3)
        plt.title('Latency Breakdown from {start} to {end} (Duration: {duration} min)'.format(start=start, end=end, duration=duration))
        fig.savefig('/sdcard/plots/lat{counter}_plot.png'.format(counter = png_counter.value))
        plt.close(fig)

        png_counter.add(1)

    print('curr time:', (time.time() - starttime))

    # if no new files, then generate the total plots and end program
    if (new_files == set()):
        print("Processing Totals...")

        utilization = list(map(lambda entry: entry[1], running_resource_blocks.value))
        min_timestamp = float('inf')
        max_timestamp = 0
        total_rbs = sum(utilization)
        for entry in running_resource_blocks.value:
            if entry[0] > max_timestamp:
                max_timestamp = math.trunc(entry[0])
            if entry[0] < min_timestamp:
                min_timestamp = math.trunc(entry[0])

        fig, _ = plt.subplots()
        kwargs = dict(alpha=0.7, bins='auto',color='#0504aa', rwidth=0.85, density=True, stacked=True)
        n, _, _ = plt.hist(x=utilization, **kwargs)
        start = datetime.datetime.fromtimestamp(min_timestamp)
        end = datetime.datetime.fromtimestamp(max_timestamp)
        duration = round((max_timestamp-min_timestamp)/60,3)
        plt.title('Channel Utilization from {start} to {end} (Duration: {duration} min)'.format(start=start, end=end, duration=duration))
        plt.xlabel('Number of Resource Blocks ({total_rbs} Total Resource Blocks in Time Window)'.format(total_rbs=total_rbs))
        plt.ylabel('Probability')
        plt.grid(axis='y', alpha=0.75)
        plt.ylim(ymax=0.5)

        fig.savefig('/sdcard/plots/total_dec.png')
        plt.close(fig)

        fig, ax = plt.subplots()
        ax.bar('Latency', waiting_accum.value, label = 'Waiting Latency: {}'.format(waiting_accum.value))
        ax.bar('Latency', tx_accum.value, bottom = waiting_accum.value, label = 'Tx Latency: {}'.format(tx_accum.value))
        ax.bar('Latency', retx_accum.value, bottom = waiting_accum.value + tx_accum.value, label = 'Retx Latency: {}'.format(retx_accum.value))
        plt.legend()
        plt.title('Latency Breakdown from {start} to {end} (Duration: {duration} min)'.format(start=start, end=end, duration=duration))
        fig.savefig('/sdcard/plots/total_lat.png')
        plt.close(fig)

        fig, ax = plt.subplots()
        for i in range(len(waiting_arr)):
            j = i + 1
            if i == 0:
                ax.bar('Packet {}'.format(j), waiting_arr[i], label = 'Waiting Latency', color='tab:blue')
                ax.bar('Packet {}'.format(j), tx_arr[i], bottom = waiting_arr[i], label = 'Tx Latency', color='tab:orange')
                ax.bar('Packet {}'.format(j), retx_arr[i], bottom = waiting_arr[i] + tx_arr[i], label = 'Retx Latency', color='tab:green')
            else:
                ax.bar('Packet {}'.format(j), waiting_arr[i],color='tab:blue')
                ax.bar('Packet {}'.format(j), tx_arr[i], bottom = waiting_arr[i], color='tab:orange')
                ax.bar('Packet {}'.format(j), retx_arr[i], bottom = waiting_arr[i] + tx_arr[i], color='tab:green')
        
        plt.xticks(rotation=35)
        plt.title('Latency from {} (1 GB / 10 mi2logs)'.format(start))
        fig.savefig('/sdcard/plots/all_lats.png')
        plt.close(fig)

        print("Stopping...")
        endtime = time.time()
        print('TOTAL TIME:', (endtime - starttime))

        ####################################################
        # UNCOMMENT THESE LINES TO GET STATS ON HEAP SPACE #
        ####################################################

        # app_id = ssc.sparkContext.applicationId
        # application_stats_request = requests.get("http://localhost:4040/api/v1/applications/{app_id}".format(app_id=app_id))
        # with open("space_results/application_statistics.json", mode='w') as writefile:
        #     writefile.write(application_stats_request.text)
        #     print('Creating application statistics file')
        
        # streaming_stats_request = requests.get("http://localhost:4040/api/v1/applications/{app_id}/streaming/statistics".format(app_id=app_id))
        # with open("space_results/streaming_statistics.json", mode='w') as writefile:
        #     writefile.write(streaming_stats_request.text)
        #     print('Creating streaming statistics file')
        
        # executor_stats_request = requests.get("http://localhost:4040/api/v1/applications/{app_id}/executors".format(app_id=app_id))
        # with open("space_results/executor_statistics.json", mode='w') as writefile:
        #     writefile.write(executor_stats_request.text)
        #     print('Creating executor statistics file')

        # snapshot = tracemalloc.take_snapshot()
        # display_top(snapshot)

        ssc.stop()

# grabs unique resource blocks from decoded log messages
def getUniqueRBs(input):
    tuples = []
    for record in input['Records']:
        if 'Num RBs' in record:
            tuples.append((input['timestamp'], record['Num RBs']))
    return tuples

# grabs unique sequential resource blocks from decoded log messages
def getUniqueSequntialRBs(input):
    tuples = []
    if len(input['Records']) >= 2:
        for i in range(0, len(input['Records']) - 2):

            curr = input['Records'][i]
            next = input['Records'][i+1]

            diff2 = (next['Frame Num'] * 10 + next['Subframe Num']) - (curr['Frame Num'] * 10 + curr['Subframe Num'])
            if diff2 == 1:
                tuples.append((input['timestamp'], curr['Num RBs']))
                if i == len(input['Records']) - 2:
                    tuples.append((input['timestamp'], next['Num RBs']))
                
    return tuples

def main():

    config = SparkConf().setAll([
        # ('spark.driver.memory','8g'),
        ("spark.executor.instances", "3"),
        ('spark.executor.cores', '5'),
        # ('spark.executor.memory', '8g'),
        ('spark.cores.max', '5'),
    ])
    
    global sc
    global ssc
    global starttime
    global processed_files
    global running_resource_blocks
    global png_counter
    global total_res_blocks_per_window
    global waiting_accum
    global tx_accum
    global retx_accum
    global waiting_arr
    global tx_arr
    global retx_arr
    waiting_arr = []
    tx_arr = []
    retx_arr = []

    # local[*] represents the number of threads to use
    sc = SparkContext("local[*]", appName="GrantStreaming", conf=config)
    sc.setLogLevel("ERROR")
    poll_interval = 1
    ssc = StreamingContext(sc, poll_interval)
    
    starttime = time.time()
    tracemalloc.start()

    # Set up the path to the folder from which spark streaming will read all new/incoming files:
    binary_mi2log_files = ssc.textFileStream('/sdcard/mobileinsight/logs/')
    
    # set up accumulators
    processed_files = sc.accumulator(set(), SetAccumulatorParam())
    running_resource_blocks = sc.accumulator([], ArrayAccumulatorParam())
    
    png_counter = sc.accumulator(1)
    total_res_blocks_per_window = sc.accumulator(0, WindowAccumulatorParam())
    
    waiting_accum = sc.accumulator(0)
    tx_accum = sc.accumulator(0)
    retx_accum = sc.accumulator(0)

    # run the spark streaming with the addToWindow function
    binary_mi2log_files.foreachRDD(addToWindow)

    ssc.start()
    ssc.awaitTermination()

def remove_folder_files(folder):    
    for filename in os.listdir(folder):
        file_path = os.path.join(folder, filename)
        try:
            if os.path.isfile(file_path) or os.path.islink(file_path):
                os.unlink(file_path)
            elif os.path.isdir(file_path):
                shutil.rmtree(file_path)
        except Exception as e:
            print('Failed to delete %s. Reason: %s' % (file_path, e))

if __name__ == "__main__":
    remove_folder_files('/sdcard/plots/')
    # remove_folder_files('/sdcard/mobileinsight/logs/')

    main()