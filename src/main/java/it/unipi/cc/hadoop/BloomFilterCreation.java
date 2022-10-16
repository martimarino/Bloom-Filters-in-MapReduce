package it.unipi.cc.hadoop;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.ArrayList;

public class BloomFilterCreation {

    public static class BloomFilterCreationMapper{
        public static class BFCMapper extends Mapper<Object, Text, IntWritable, BloomFilter> {
            ArrayList<BloomFilter> bloomFilters = new ArrayList<>();
            final int n_rates = 10; //number of rates
            //creation of the 10 blooomfilters basing on corresponding m,k
            public void init(Context context) {

                for (int i = 0; i < n_rates; i++) {
                    int m = Integer.parseInt(context.getConfiguration().get("filter_" + i + "_m")); //need to define after
                    int k = Integer.parseInt(context.getConfiguration().get("filter_" + i + "_k")); //need to define after
                    bloomFilters.add(i, new BloomFilter(m, k));
                }
            }
            //insert into bloomfilter corresponding to the calculated rating, the id of the film, for each film
            public void map(Object object, Text value, Context context){
                    String[] tokens = value.toString().split("\t"); //id (0) , rating (1)
                    int roundedRating = (int) Math.round(Double.parseDouble(tokens[1]))-1;
                    bloomFilters.get(roundedRating).insert(tokens[0]);
            }
            public void emits(Context context) throws IOException, InterruptedException {
                for(int i = 0; i < n_rates; i++)
                    context.write(new IntWritable(i+1), bloomFilters.get(i));
            }
        }


        public static class BFCReducer extends Reducer<IntWritable, BloomFilter, IntWritable, BloomFilter> { //<rating, bloomfilters, rating, bloomfilter
            BloomFilter result;
            //for all the bloomfilter received, doing the or
            public void reduce(IntWritable key, Iterable<BloomFilter> values, Context context) throws IOException, InterruptedException {
                result = new BloomFilter(values.iterator().next());
                while(values.iterator().hasNext()) {
                    result.or(values.iterator().next().bf);
                }

                context.write(key, result); //<rating, bloomfilter>
            }
        }

        public static boolean main(Job job) throws IOException, InterruptedException, ClassNotFoundException {
            Configuration conf = job.getConfiguration();

            job.setJarByClass(BloomFilterCreation.class);

            job.setMapperClass(BFCMapper.class);
            job.setReducerClass(BFCReducer.class);

            // mapper's output key and output value
            job.setMapOutputKeyClass(IntWritable.class);
            job.setMapOutputValueClass(BloomFilter.class);

            // reducer's output key and output value
            job.setOutputKeyClass(IntWritable.class);
            job.setOutputValueClass(BloomFilter.class);

            FileInputFormat.addInputPath(job, new Path(conf.get("input.path")));
            FileOutputFormat.setOutputPath(job, new Path(conf.get("output.path")));

            return job.waitForCompletion(true);
        }
    }


}
