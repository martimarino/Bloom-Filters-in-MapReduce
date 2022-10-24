# Bloom Filters in MapReduce

A bloom filter is a space-efficient probabilistic data structure that is used for membership testing.

To keep it simple, its main usage is to “remember” which keys were given to it. 

A bloom filter is space-efficient, meaning that it needs very little memory to remember what you gave it. Actually it has a fixed size, so you even get to decide how much memory you want it to use, independently of how many keys you will pass to it later. Of course this comes at a certain cost, which is the possibility of having false positives. But there can never be false negatives. 

![image](https://user-images.githubusercontent.com/45880539/192534193-bf8f4aae-b2dc-447e-b42f-971ae6ff4dc3.png)

A bloom filter is a bit-vector with 𝑚 elements. It uses 𝑘 hash functions to map 𝑛 keys to the 𝑚 elements of the bit-vector. Given a key 𝑖𝑑𝑖 , every hash function ℎ1,…, ℎ𝑘 computes the corresponding output positions, and sets the corresponding bit in that position to 1, if it is equal to 0.
Let’s consider a Bloom filter with the following characteristics:
- 𝑚 : number of bits in the bit-vector,
- 𝑘 : number of hash functions,
- 𝑛 : number of keys added for membership testing,
- 𝑝 : false positive rate (probability between 0 and 1).
The relations between theses values can be expressed as:

![image](https://user-images.githubusercontent.com/45880539/192534832-37940420-4a60-49b2-beb4-5872f6ce2ed0.png)

To design a bloom filter with a given false positive rate 𝑝, you need to estimate the number of keys 𝑛 to be added to the bloom filter, then compute the number of bits 𝑚 in the bloom filter and finally compute the number of hash functions 𝑘 to use.

A bloom filter has been builded over the ratings of movies listed in the IMDb datasets. The average ratings are rounded to the closest integer value, and a bloom filter has been computed for each rating value.
In Hadoop implementation the following classes has been used:
- org.apache.hadoop.mapreduce.lib.input.NLineInputFormat: splits N lines of input as one split;
- org.apache.hadoop.util.hash.Hash.MURMUR_HASH: the hash function family to use.
In the Spark implementation, analogous classes has been used.
