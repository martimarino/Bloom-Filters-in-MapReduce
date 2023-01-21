import sys
import os
import math
import time

import mmh3
from bitarray import bitarray

from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession

from pyspark.accumulators import AccumulatorParam


# Accumulator array of False Positives counts, each position in the accumulator corresponds to the false positive
# count of each bloom filter
class FalsePositiveAccumulatorParam(AccumulatorParam):
    def zero(self, value):
        return [0] * len(value)

    def addInPlace(self, acc, fp):
        for i in range(len(acc)):
            acc[i] += fp[i]
        return acc


# Function that computes m (the dimension of each bloom filter)
def compute_m(count, fpr):
    return math.ceil(-((count * math.log(fpr)) / math.pow(math.log(2), 2))), count


# Function that computes k (the number of hashes to compute for each title)
def compute_k(fpr):
    return int(-math.log(fpr) / math.log(2))


# Function that creates the bloom filters. It takes as input the list of titles for each vote and populate a
# local bloom filter of that vote
def create_bf(row):
    global broadcast_m, broadcast_k
    key = row[0]
    titles = row[1]
    bf = bitarray(broadcast_m.value[key - 1])
    bf.setall(0)
    for title in titles:
        for i in range(broadcast_k.value):
            hash_i = abs(mmh3.hash(title, i)) % broadcast_m.value[key - 1]
            bf[hash_i] = True
    return key, bf


# Function that populates the accumulator of false positives. It takes as input a row of the RDD, computes the k
# hashes for each film and check the all bloom filters different from its own, finding false positives and
# increasing the respective position in the accumulator
def add_fp(row):
    global fps_acc, broadcast_bfs
    key = row[0]
    title = row[1]
    counter = [0 for _ in range(10)]
    for i in range(10):
        pos_count = 0
        if i == key - 1:
            continue
        for j in range(broadcast_k.value):
            hash_j = mmh3.hash(title, j) % broadcast_m.value[i]
            if broadcast_bfs.value[i][hash_j]:
                pos_count += 1
        if pos_count == broadcast_k.value:
            counter[i] = 1
    fps_acc += counter


if __name__ == "__main__":

    # Checks if the number of inputs in right
    if len(sys.argv) != 3:
        print("Input list: <script.py> <dataset> <fpr>")
        exit()

    # Save the dataset path and the False Positive Rate
    path = sys.argv[1]
    fpr = float(sys.argv[2])

    # Set the working environment to the virtual one installed on the VMs
    os.environ['PYSPARK_PYTHON'] = './pyspark_venv/bin/python'

    # Set in the configuration the master as yarn cluster and the name of the application
    conf = (SparkConf()
            .setMaster("yarn")
            .setAppName("BFSpark"))

    # Initialize a new Spark Context to use for the execution of the script
    sc = SparkContext(conf=conf)

    spark = SparkSession(sc)

    # Set the log level to 'WARN' in order to have a cleaner output
    spark.sparkContext.setLogLevel('WARN')

    # STAGE 1
    # loading the csv removing numVotes
    print("\n\nStarting STAGE 1...")
    # Start the timer for stage 1
    start1 = time.time()
    # Load the csv removing numVotes
    rdd = spark.read.csv(path, sep='\t', header=True).drop('numVotes').select("tconst", "averageRating").rdd

    # Map the dataset as < vote, title > and save cache it
    new_dataset = rdd.map(lambda x: (int(round(float(x[1]))), x[0])).cache()

    print("Counting the films per vote...")
    # Counting the number of movies per vote
    vote_count = new_dataset.map(lambda x: (x[0], 1)).reduceByKey(lambda x, y: x + y).sortByKey()

    print("Computing M values...")
    # Computing the M and N values for each vote
    mn = vote_count.map(lambda x: (x[0], compute_m(x[1], fpr))).sortByKey().values().collect()

    print("Computing movie counts per vote...")
    # Zip the M and N in two different arrays
    m_values, tot_values = map(list, zip(*mn))

    print("Broadcasting variables...")
    # Compute k
    k = compute_k(fpr)

    # Broadcasts the array of M values, N values and K to all workers
    broadcast_m = sc.broadcast(m_values)
    print("M: " + str(m_values))
    broadcast_k = sc.broadcast(k)
    print("K: " + str(k))
    broadcast_tot = sc.broadcast(tot_values)
    print("N: " + str(tot_values))

    # End the timer for stage 1 and print the execution time
    elapsed_time_stage1 = time.time() - start1
    print(elapsed_time_stage1, "seconds elapsed for completing STAGE 1\n")

    # STAGE 2
    print("Starting STAGE 2...")
    # Start the timer for stage 2
    start2 = time.time()
    print("Grouping titles...")

    # Groups titles in lists by their key
    title_lists = new_dataset.groupByKey()

    print("Creating bloom filters...")
    # Compute the hashes for the titles of each list and populate local bloom filters, the aggregate them
    bloom_filters = title_lists.map(create_bf).reduceByKey(lambda x, y: x | y).sortByKey().values().collect()

    # Print the length of each bloom filter just populated
    j = 0
    for i in bloom_filters:
        print("[" + str(j + 1) + "]: " + str(len(i)))
        j = j + 1

    print("Broadcasting bloom filters...")
    # Instantiate globally the bloom filters
    broadcast_bfs = sc.broadcast(bloom_filters)

    # End the timer for stage 2 and print the execution time
    elapsed_time_stage2 = time.time() - start2
    print(elapsed_time_stage2, "seconds elapsed for completing STAGE 2\n")

    # STAGE 3
    print("Starting STAGE 3...")
    # Start the timer for stage 3
    start3 = time.time()
    print("Checking False Positive Rates...")
    # Initialise a global accumulator for the false positives count
    fps_acc = sc.accumulator([0] * 10, FalsePositiveAccumulatorParam())

    # Iterate the dataset and recompute the hashes of each title to check if it's a false positive in other bloom
    # filters
    new_dataset.foreach(add_fp)

    # Compute the total sum of films
    tot = sum(broadcast_tot.value)

    # Print the 10 False Positive Rates
    for i in range(10):
        print("[" + str(i + 1) + "]: " + str(fps_acc.value[i] / (tot - broadcast_tot.value[i])))

    # Remove the global variables
    broadcast_m.destroy()
    broadcast_k.destroy()
    broadcast_tot.destroy()
    broadcast_bfs.destroy()

    # End the timer for stage 3 and print the execution time
    elapsed_time_stage3 = time.time() - start3
    print(elapsed_time_stage3, "seconds elapsed for completing STAGE 3\n\n")
