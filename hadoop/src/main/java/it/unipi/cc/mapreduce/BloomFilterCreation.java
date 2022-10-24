package it.unipi.cc.mapreduce;

import it.unipi.cc.model.BloomFilter;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.util.hash.MurmurHash;

import java.io.IOException;
import java.util.BitSet;

import static org.apache.hadoop.util.hash.Hash.MURMUR_HASH;

public class BloomFilterCreation {

    public static class BFCMapper extends Mapper<Object, Text, IntWritable, IntWritable> {

        private static int k;
        private static final IntWritable outputKey = new IntWritable();
        private static final IntWritable outputValue = new IntWritable();

        //creation of the 10 bloom filters basing on corresponding m,k
        @Override
        protected void setup(Context context) {
            k = Integer.parseInt(context.getConfiguration().get("filter_k")); //need to define after
        }

        //insert into bloom filter corresponding to the calculated rating, the id of the film, for each film
        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {

            if(value.toString().startsWith("tconst"))
                    return;
            String[] tokens = value.toString().split("\t"); //id (0) , rating (1)
            int roundedRating = (int) Math.round(Double.parseDouble(tokens[1]));
            int m = Integer.parseInt(context.getConfiguration().get("filter_" + roundedRating + "_m"));

            for(int i = 0; i < k; i++) {
                int index = Math.abs(MurmurHash.getInstance(MURMUR_HASH).hash(tokens[0].getBytes(), i) % m);

                outputKey.set(roundedRating);
                outputValue.set(index);
                context.write(outputKey, outputValue);
            }
        }
    }

    public static class BFCReducer extends Reducer<IntWritable, IntWritable, IntWritable, BloomFilter> { //<rating, bloom filters, rating, bloomfilter
        private static MultipleOutputs mos;
        private static int k;

        //for all the bloom filter received, doing the or
        @Override
        protected void setup(Context context) {
            mos = new MultipleOutputs(context);
            k = Integer.parseInt(context.getConfiguration().get("filter_k")); //need to define after
        }
        @Override
        public void reduce(IntWritable key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int m = Integer.parseInt(context.getConfiguration().get("filter_" + key + "_m"));
            BitSet bitset = new BitSet(m);
            for(IntWritable i: values)
                bitset.set(i.get());
            BloomFilter bloomfilter = new BloomFilter(k, m, bitset);
            mos.write(key, bloomfilter, "rate"+key);
        }
        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            mos.close();
        }
    }

}
