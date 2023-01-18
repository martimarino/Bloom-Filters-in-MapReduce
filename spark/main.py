import sys
import os
import math
import time

import mmh3
from bitarray import bitarray

from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession

from pyspark.accumulators import AccumulatorParam


class BloomFilterAccumulatorParam(AccumulatorParam):
    def zero(self, value):
        return [0] * len(value)

    def addInPlace(self, acc, fp):
        for i in range(len(acc)):
            acc[i] += fp[i]
        return acc

    # def addInPlace(self, fp, index):
    #     if type(index) == list:
    #         fp[index[0]] += 1
    #     else:
    #         fp += index
    #     return fp


def fill_bitarrays(row):
    global broadcast_m, broadcast_k, bitarray_acc
    key = row[0]
    title = row[1]
    for i in range(broadcast_k.value):
        hash_i = abs(mmh3.hash(title, i)) % broadcast_m.value[key - 1]
        bitarray_acc[key][hash_i] = True


def compute_m(count, fpr):
    return math.ceil(-((count * math.log(fpr)) / math.pow(math.log(2), 2))), count


def compute_k(fpr):
    return int(-math.log(fpr) / math.log(2))


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


def add_bf(row):
    global bfs_acc
    key = row[0]
    title = row[1]
    counter = [0 for _ in range(10)]
    for i in range(10):
        pos_count = 0
        if i == key - 1:
            continue
        for j in range(broadcast_k.value):
            hash_j = mmh3.hash(title, j) % broadcast_m.value[i]
            if rdd_bf.value[i][hash_j]:
                pos_count += 1
        if pos_count == broadcast_k.value:
            counter[i] = 1
            # bfs_acc += [i]
    bfs_acc += counter


if __name__ == "__main__":

    if len(sys.argv) != 3:
        print("Usage: main.py <dataset> <fp>")
        exit()

    path = sys.argv[1]
    fpr = float(sys.argv[2])

    os.environ['PYSPARK_PYTHON'] = './pyspark_venv/bin/python'

    conf = (SparkConf()
            .setMaster("yarn")  # yarn/local[*]
            .setAppName("BFSpark"))

    # initialize a new Spark Context to use for the execution of the script
    sc = SparkContext(conf=conf)

    spark = SparkSession(sc)
    spark.sparkContext.setLogLevel('WARN')

    # STAGE 1
    # loading the csv removing numVotes
    results = "RESULTS:\n\n"
    print("\n\nStarting STAGE 1...")
    start1 = time.time()
    rdd = spark.read.csv(path, sep='\t', header=True).drop('numVotes').select("tconst", "averageRating").rdd
    new_dataset = rdd.map(lambda x: (int(round(float(x[1]))), x[0])).cache()

    print("Counting the films per vote...")
    # counting the number of movies per vote
    vote_count = new_dataset.map(lambda x: (x[0], 1)).reduceByKey(lambda x, y: x + y).sortByKey()

    print("Computing M values...")
    # computing the M value for each vote
    mn = vote_count.map(lambda x: (x[0], compute_m(x[1], fpr))).sortByKey().values().collect()

    print("Computing movie counts per vote...")
    # collects an ordered array of movie counts per vote
    m_values, tot_values = map(list, zip(*mn))

    print("Broadcasting variables...")
    # compute k
    k = compute_k(fpr)

    # broadcasts the vector of M values and K to all workers
    broadcast_m = sc.broadcast(m_values)
    print("M: " + str(m_values))
    broadcast_k = sc.broadcast(k)
    print("K: " + str(k))
    broadcast_tot = sc.broadcast(tot_values)
    print("N: " + str(tot_values))
    results = results + "M: " + str(m_values) + "\n\n" + "K: " + str(k) + "\n\n" + "N: " + str(tot_values) + "\n\n"

    elapsed_time_stage1 = time.time() - start1
    print(elapsed_time_stage1, "seconds elapsed for completing STAGE 1\n")
    p = str(round(elapsed_time_stage1, 4))
    results = results + "Elapsed time for STAGE 1:\n" + str(round(elapsed_time_stage1, 4)) + " seconds\n\n"

    # STAGE 2
    print("Starting STAGE 2...")
    start2 = time.time()
    print("Grouping titles...")
    title_lists = new_dataset.groupByKey()

    print("Creating bloom filters...")
    bloom_filters = title_lists.map(create_bf).reduceByKey(lambda x, y: x | y).sortByKey().values().collect()

    results = results + "Bloom filters dimensions:\n"

    j = 0
    for i in bloom_filters:
        print("[" + str(j + 1) + "]: " + str(len(i)))
        results = results + "[" + str(j + 1) + "]: " + str(len(i)) + "\n"
        j = j + 1

    results = results + "\n"

    # instantiate globally the bloom filters
    print("Broadcasting bloom filters...")
    rdd_bf = sc.broadcast(bloom_filters)

    elapsed_time_stage2 = time.time() - start2
    print(elapsed_time_stage2, "seconds elapsed for completing STAGE 2\n")
    p = p + " " + str(round(elapsed_time_stage2, 4)) + "\n"
    results = results + "Elapsed time for STAGE 2:\n" + str(round(elapsed_time_stage2, 4)) + " seconds\n\n"

    # STAGE 3
    print("Starting STAGE 3...")
    start3 = time.time()
    print("Checking False Positive Rates...")
    bfs_acc = sc.accumulator([0] * 10, BloomFilterAccumulatorParam())
    new_dataset.foreach(add_bf)
    tot = sum(broadcast_tot.value)

    for i in range(10):
        print("[" + str(i + 1) + "]: " + str(bfs_acc.value[i] / (tot - broadcast_tot.value[i])))
        results = results + "[" + str(i + 1) + "]: " + str(bfs_acc.value[i] / (tot - broadcast_tot.value[i])) + "\n"
        p = p + "[" + str(i + 1) + "]: " + str(bfs_acc.value[i] / (tot - broadcast_tot.value[i])) + " " + str(
            bfs_acc.value[i]) + "\n"

    results = results + "\n"
    p = p + "\n"

    # remove the global variables
    broadcast_m.destroy()
    broadcast_k.destroy()

    elapsed_time_stage3 = time.time() - start3
    print(elapsed_time_stage3, "seconds elapsed for completing STAGE 3\n\n")
    p = p + str(round(elapsed_time_stage3, 4)) + "\n\n"
    results = results + "Elapsed time for STAGE 3:\n" + str(round(elapsed_time_stage3, 4)) + " seconds\n\n"

    # Open the file in write mode
    with open("results.txt", "w") as f:
        # Write the text to the file
        f.write(results)

    with open("p" + str(fpr) + ".txt", "w") as f:
        # Write the text to the file
        f.write(p)