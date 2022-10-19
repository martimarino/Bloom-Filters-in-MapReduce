package it.unipi.cc.hadoop.mapreduce;

import it.unipi.cc.hadoop.model.BloomFilter;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.hash.MurmurHash;

import java.io.IOException;
import java.util.ArrayList;
import java.util.BitSet;

import static org.apache.hadoop.util.hash.Hash.MURMUR_HASH;

public class BloomFilterCreation {

    public static class BFCMapper extends Mapper<Object, Text, IntWritable, IntWritable> {
        //private static ArrayList<BloomFilter> bloomFilters = new ArrayList<>();
        private static int k;
        private static final IntWritable outputKey = new IntWritable();
        private static final IntWritable outputValue = new IntWritable();

        //creation of the 10 blooom filters basing on corresponding m,k
        public void setup(Context context) {

            k = Integer.parseInt(context.getConfiguration().get("filter_k")); //need to define after

        }
        //insert into bloomfilter corresponding to the calculated rating, the id of the film, for each film
        public void map(Object object, Text value, Context context) throws IOException, InterruptedException {

            System.out.println("MAP");
            if(value.toString().startsWith("tconst"))
                    return;
            String[] tokens = value.toString().split("\t"); //id (0) , rating (1)
            int roundedRating = (int) Math.round(Double.parseDouble(tokens[1]))-1;
            System.out.println("RR:" + roundedRating);
            int m = Integer.parseInt(context.getConfiguration().get("filter_" + roundedRating + "_m"));
            System.out.println("M: " + m);
            for(int i = 0; i < k; i++) {
                int index = Math.abs(MurmurHash.getInstance(MURMUR_HASH).hash(tokens[0].getBytes(), i) % m);
//                System.out.println("INDEX: " + index);
                outputKey.set(roundedRating);
                outputValue.set(index);
                try {
                    context.write(outputKey, outputValue);
                }catch(Exception e)
                {
                    e.printStackTrace();
                }
                System.out.println("END");
            }
            System.out.println("END MAP");
        }

    }


    public static class BFCReducer extends Reducer<IntWritable, IntWritable, IntWritable, BloomFilter> { //<rating, bloomfilters, rating, bloomfilter
        private static BloomFilter result;
        private static int k;
        //for all the bloomfilter received, doing the or
        public void setup(Context context) {

            k = Integer.parseInt(context.getConfiguration().get("filter_k")); //need to define after

        }
        public void reduce(IntWritable key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int m = Integer.parseInt(context.getConfiguration().get("filter_" + key + "_m"));
            BitSet bitset = new BitSet(m);
            for(IntWritable i: values)
                bitset.set(i.get());
            BloomFilter bloomfilter = new BloomFilter(k, m, bitset);
            context.write(key, bloomfilter);
        }
    }

}
