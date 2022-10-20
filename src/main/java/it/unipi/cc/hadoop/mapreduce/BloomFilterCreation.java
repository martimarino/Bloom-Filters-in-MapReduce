package it.unipi.cc.hadoop.mapreduce;

import it.unipi.cc.hadoop.Driver;
import it.unipi.cc.hadoop.model.BloomFilter;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
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
        public void map(Object object, Text value, Context context) throws IOException, InterruptedException {
            Driver.print("BloomFilterCreation -> map");

            Driver.print("MAP");
            if(value.toString().startsWith("tconst"))
                    return;
            String[] tokens = value.toString().split("\t"); //id (0) , rating (1)
            int roundedRating = (int) Math.round(Double.parseDouble(tokens[1]));
            Driver.print("RR:" + roundedRating);
            int m = Integer.parseInt(context.getConfiguration().get("filter_" + roundedRating + "_m"));
            Driver.print("M: " + m);
            for(int i = 0; i < k; i++) {
                int index = Math.abs(MurmurHash.getInstance(MURMUR_HASH).hash(tokens[0].getBytes(), i) % m);

                outputKey.set(roundedRating);
                outputValue.set(index);
                context.write(outputKey, outputValue);
                Driver.print("END");
            }
            Driver.print("END MAP");
        }

    }


    public static class BFCReducer extends Reducer<IntWritable, IntWritable, IntWritable, BloomFilter> { //<rating, bloom filters, rating, bloomfilter
        private static int k;

        //for all the bloom filter received, doing the or
        @Override
        protected void setup(Context context) {

            k = Integer.parseInt(context.getConfiguration().get("filter_k")); //need to define after
            System.out.println("K:" + k);
        }
        @Override
        public void reduce(IntWritable key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            System.out.println("BloomFilterCreation -> reduce");
            int m = Integer.parseInt(context.getConfiguration().get("filter_" + key + "_m"));
            BitSet bitset = new BitSet(m);
            for(IntWritable i: values)
                bitset.set(i.get());
            BloomFilter bloomfilter = new BloomFilter(k, m, bitset);
            context.write(key, bloomfilter);
        }
    }

}
