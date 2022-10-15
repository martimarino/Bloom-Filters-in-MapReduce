package it.unipi.cc.hadoop;

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

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String record = value.toString();
            if (record == null || record.length() != 3)
                return;

            String[] tokens = record.split("\t");

            int rate = Math.round(Float.parseFloat(tokens[1])); // <title, rating, numVotes>
            counter[rate -1]++;

            for(int i: counter)
                context.write(new IntWritable(i+1), new IntWritable(counter[i])); //<key, value>
        }
    }

    public static class PCReducer extends Reducer<IntWritable, IntWritable, IntWritable, Text> {

        public void reduce(IntWritable key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int n = 0;
            while(values.iterator().hasNext())
                n += values.iterator().next().get();

            double p = 0.01; //parametri configurazione ?
            int m = (int) (- ( n * Math.log(p) ) / (Math.pow(Math.log(2),2.0)));
            int k = (int) ((m/n) * Math.log(2));

            Text value = new Text(n + "\t" + m + "\t" + k);
            context.write(key, value);
        }
    }
    public static boolean main(Job job) throws IOException, InterruptedException, ClassNotFoundException {
        Configuration conf = job.getConfiguration();

        job.setJarByClass(ParameterCalibration.class);

        job.setMapperClass(PCMapper.class);
        job.setReducerClass(PCReducer.class);

        // mapper's output key and output value
        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(IntWritable.class);

        // reducer's output key and output value
        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(Text.class);

        double p = 0.01; //configurazione da file?
        job.getConfiguration().setDouble("p", p);

        //path da passare come argomento(?)
        FileInputFormat.addInputPath(job, new Path(conf.get("input.path"))); //input file that needs to be used by MapReduce program
        FileOutputFormat.setOutputPath(job, new Path(conf.get("output.path"))); //output file

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        return job.waitForCompletion(true);
    }
}
