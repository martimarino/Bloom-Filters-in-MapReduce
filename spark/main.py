import sys
import math
import pickle
import mmh3
from bitarray import bitarray

import findspark
findspark.init()

from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession


def compute_m(count, p):
    return math.ceil(-((count * math.log(p)) / math.pow(math.log(2), 2)))


def compute_k(p):
    return int(-math.log(p) / math.log(2))


def positions(rows):
    global broadcast_k, broadcast_m
    vote = rows[1]
    pos = []
    for i in range(broadcast_k.value):
        hash_i = mmh3.hash(rows[0], i) % broadcast_m.value[int(round(float(rows[1]))) - 1]
        pos.append(hash_i)
    return vote, pos


def create_bf(rows):
    global broadcast_m
    vote = rows[0]
    hash_list = rows[1]
    bf = bitarray(broadcast_m.value[vote - 1])
    bf.setall(0)
    for v in hash_list:
        bf[v] = True
    return bf


def check_bf(rows):
    global broadcast_m, broadcast_k
    vote = rows[1]
    counter = [0 for _ in range(10)]
    for i in range(10):
        pos_count = 0
        if i == vote - 1:
            continue
        for j in range(broadcast_k.value):
            hash_j = mmh3.hash(rows[0], j) % broadcast_m.value[int(round(float(vote))) - 1]
            if rdd_bf.value[i + 1][hash_j]:
                pos_count += 1
        if pos_count == broadcast_k.value:
            counter[i] = 1
    return counter


if __name__ == "__main__":

    if len(sys.argv) != 4:
        print("Usage: main.py <dataset> <0/1> <fp>")
        exit()

    path = sys.argv[1]
    output = sys.argv[2]
    p = float(sys.argv[3])

    # application name
    name = "SparkBloomFilters"

    # initialize a new Spark Context to use for the execution of the script
    sc = SparkContext(appName=name, master="yarn")

    # STAGE 1
    # loading the csv removing numVotes
    spark = SparkSession(sc)
    rdd = spark.read.csv(path, sep='\t', header=True).drop('numVotes').select("tconst", "averageRating").rdd

    # counting the number of movies per vote
    vote_count = rdd.map(lambda x: (int(round(float(x[1]))), 1)).reduceByKey(lambda x, y: x + y).sortByKey()

    # computing the M value for each vote
    m_values = vote_count.map(lambda x: (x[0], compute_m(x[1], p))).sortByKey().values().collect()

    # collects an ordered array of movie counts per vote
    tot_values = vote_count.values().collect()

    # compute k
    k = compute_k(p)

    # broadcasts the vector of M values and K to all workers
    broadcast_m = sc.broadcast(m_values)
    broadcast_k = sc.broadcast(k)
    broadcast_tot = sc.broadcast(tot_values)

    # STAGE 2
    # find the hashes (positions) for each movie and aggregate them
    key_hashes = rdd.map(positions).reduceByKey(lambda x, y: x + y)

    # create the bloom filters
    bloom_filters = key_hashes.map(create_bf).sortByKey().collect()

    # remove the global variables
    # broadcast_m.destroy()
    # broadcast_k.destroy()

    # instantiate globally the bloom filters
    rdd_bf = sc.broadcast(bloom_filters)

    # STAGE 3
    fp_set = rdd.map(check_bf).reduce(lambda x, y: x + y).sortByKey()

    print(fp_set)
