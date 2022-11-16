import sys
import math
import time

import mmh3
import numpy as np
from bitarray import bitarray

import findspark

findspark.init()

from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession


def compute_m(count, p):
    return math.ceil(-((count * math.log(p)) / math.pow(math.log(2), 2)))


def compute_k(p):
    return int(-math.log(p) / math.log(2))


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


def check_bf(row):
    global broadcast_m, broadcast_k, rdd_bf
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
    return key, np.array(counter, dtype=object)


if __name__ == "__main__":

    # if len(sys.argv) != 4:
    #     print("Usage: main.py <dataset> <0/1> <fp>")
    #     exit()

    # path = sys.argv[1]
    # output = sys.argv[2]
    # p = float(sys.argv[3])

    conf = (SparkConf()
            .setMaster("yarn")
            .setAppName("BFSpark"))

    # initialize a new Spark Context to use for the execution of the script
    sc = SparkContext(conf=conf)

    p = 0.01
    path = "dataset.tsv"

    spark = SparkSession(sc)
    spark.sparkContext.setLogLevel('WARN')

    # STAGE 1
    # loading the csv removing numVotes
    print("Starting STAGE 1...")
    start1 = time.time()
    rdd = spark.read.csv(path, sep='\t', header=True).drop('numVotes').select("tconst", "averageRating").rdd
    new_dataset = rdd.map(lambda x: (int(round(float(x[1]))), x[0]))

    print("Counting the films per vote...")
    # counting the number of movies per vote
    vote_count = rdd.map(lambda x: (int(round(float(x[1]))), 1)).reduceByKey(lambda x, y: x + y).sortByKey()

    print("Computing M values...")
    # computing the M value for each vote
    m_values = vote_count.map(lambda x: (x[0], compute_m(x[1], p))).sortByKey().values().collect()

    print("Computing movie counts per vote...")
    # collects an ordered array of movie counts per vote
    tot_values = vote_count.values().collect()

    print("Broadcasting variables...")
    # compute k
    k = compute_k(p)

    # broadcasts the vector of M values and K to all workers
    broadcast_m = sc.broadcast(m_values)
    print(m_values)
    broadcast_k = sc.broadcast(k)
    print(k)
    broadcast_tot = sc.broadcast(tot_values)
    print(tot_values)

    elapsed_time_stage1 = time.time() - start1
    print(elapsed_time_stage1, "seconds")

    # STAGE 2
    print("Starting STAGE 2...")
    start2 = time.time()
    print("Grouping titles...")
    title_lists = new_dataset.groupByKey()

    print("Creating bloom filters...")
    bloom_filters = title_lists.map(create_bf).reduceByKey(lambda x, y: x & y).sortByKey().values().collect()

    for i in bloom_filters:
        print(len(i))

    # remove the global variables
    # broadcast_m.destroy()
    # broadcast_k.destroy()

    # instantiate globally the bloom filters
    print("Broadcasting bloom filters...")
    rdd_bf = sc.broadcast(bloom_filters)

    elapsed_time_stage2 = time.time() - start2
    print(elapsed_time_stage2, "seconds")

    # STAGE 3
    print("Starting STAGE 3...")
    start3 = time.time()
    print("Checking False Positives...")
    fp_set = new_dataset.map(check_bf).reduce(lambda x, y: np.add(np.array(x, dtype=object), np.array(y, dtype=object)))

    for i in fp_set:
        print(i)

    elapsed_time_stage3 = time.time() - start3
    print(elapsed_time_stage3, "seconds")
