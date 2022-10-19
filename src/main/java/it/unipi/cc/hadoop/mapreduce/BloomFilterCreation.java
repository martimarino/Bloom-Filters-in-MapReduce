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

import java.io.IOException;
import java.util.ArrayList;

public class BloomFilterCreation {

    public static class BFCMapper extends Mapper<Object, Text, IntWritable, BloomFilter> {
        private static ArrayList<BloomFilter> bloomFilters = new ArrayList<>();
        private static int n_rates; //number of rates

        private static final IntWritable outputKey = new IntWritable();
        private static BloomFilter outputVal = new BloomFilter();


        //creation of the 10 blooom filters basing on corresponding m,k
        public void setup(Context context) {

            n_rates = context.getConfiguration().getInt("n_rates", 0);

            for (int i = 0; i < n_rates; i++) {
                int m = Integer.parseInt(context.getConfiguration().get("filter_" + i + "_m")); //need to define after
                int k = Integer.parseInt(context.getConfiguration().get("filter_" + i + "_k")); //need to define after
                BloomFilter filter = new BloomFilter();
                filter.setM(new IntWritable(m));
                filter.setK(new IntWritable(k));
                bloomFilters.add(i, filter);
            }
        }
        //insert into bloomfilter corresponding to the calculated rating, the id of the film, for each film
        public void map(Object object, Text value, Context context){
                String[] tokens = value.toString().split("\t"); //id (0) , rating (1)
                int roundedRating = (int) Math.round(Double.parseDouble(tokens[1]))-1;
                bloomFilters.get(roundedRating).insert(tokens[0]);
        }
        public void cleanup(Context context) throws IOException, InterruptedException {
            for(int i = 0; i < n_rates; i++) {
                outputKey.set(i + 1);
                context.write(outputKey, bloomFilters.get(i));
            }
        }
    }


    public static class BFCReducer extends Reducer<IntWritable, BloomFilter, IntWritable, BloomFilter> { //<rating, bloomfilters, rating, bloomfilter
        private static BloomFilter result;
        //for all the bloomfilter received, doing the or
        public void reduce(IntWritable key, Iterable<BloomFilter> values, Context context) throws IOException, InterruptedException {
            result = new BloomFilter(values.iterator().next());
            while (values.iterator().hasNext()) {
                result.or(values.iterator().next().getBitSet());
            }
            context.write(key, result); //<rating, bloomfilter>
        }
    }

}
