package it.unipi.cc.hadoop.mapreduce;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.IntWritable;

import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;

public class ParameterCalibration {
    public static class PCMapper extends Mapper<Object, Text, IntWritable, IntWritable> {
        private int[] counter = new int[10];

        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String record = value.toString();
            if (record == null || record.startsWith("tconst"))
                return;
            String[] tokens = record.split("\t");

            int rate = Math.round(Float.parseFloat(tokens[1])); // <title, rating, numVotes>
            counter[rate - 1]++;
        }

        @Override
        public void cleanup(Context context) throws IOException, InterruptedException {
            for(int i=0; i<counter.length; i++)
                context.write(new IntWritable(i + 1), new IntWritable(counter[i])); //<key, value>
        }
    }

    public static class PCReducer extends Reducer<IntWritable, IntWritable, IntWritable, Text> {

        private static double p;
        @Override
        public void setup(Context context) {
            p = context.getConfiguration().getDouble("p", 0.01);
        }
        public void reduce(IntWritable key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int n = 0;
            while(values.iterator().hasNext())
                n += values.iterator().next().get();

            int m = (int) (- (n * Math.log(p)) / (Math.pow(Math.log(2),2)));
            int k = (int) ((m/n) * Math.log(2));

            Text value = new Text(m + "\t" + k);
            context.write(key, value);
        }
    }

}
