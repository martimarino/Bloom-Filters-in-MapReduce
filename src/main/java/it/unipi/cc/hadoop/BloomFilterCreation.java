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
import org.apache.hadoop.util.GenericOptionsParser;

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

        public static boolean main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
            Configuration conf = new Configuration();

            String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();

            //not expected number of arguments
            if (otherArgs.length != 2) {
                System.err.println("Usage: BloomFilterCreation <input> <output>");
                System.exit(2);
            }

            Job job = Job.getInstance(conf, "BloomFilterCreation");
            job.setJarByClass(BloomFilterCreation.class);

            job.setMapperClass(BFCMapper.class);
            job.setReducerClass(BFCReducer.class);

            // mapper's output key and output value
            job.setMapOutputKeyClass(IntWritable.class);
            job.setMapOutputValueClass(BloomFilter.class);

            // reducer's output key and output value
            job.setOutputKeyClass(IntWritable.class);
            job.setOutputValueClass(BloomFilter.class);

            FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
            FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));

            return job.waitForCompletion(true);
        }
    }


}
