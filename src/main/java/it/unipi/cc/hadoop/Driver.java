package it.unipi.cc.hadoop;

import it.unipi.cc.hadoop.mapreduce.BloomFilterCreation;
import it.unipi.cc.hadoop.mapreduce.ParameterCalibration;
import it.unipi.cc.hadoop.mapreduce.ParameterValidation;
import it.unipi.cc.hadoop.model.BloomFilter;
import it.unipi.cc.hadoop.model.Parameters;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;

public class Driver {

    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {

        Configuration conf = new Configuration();

        Parameters parameters = new Parameters("../../../conf.properties");

        String DIR = parameters.getOutputPath()+"/";
        conf.set("input.path", parameters.getInputPath());
        conf.set("output.path", parameters.getOutputPath());
        conf.setDouble("p", parameters.getP());

        conf.set("output.parameter-calibration", DIR + "parameter-calibration");
        conf.set("output.bloom-filters-creation", DIR + "bloom-filters-creation");
        conf.set("output.parameter-validation", DIR + "parameter-validation");

        // Clean HDFS workspace
        FileSystem fs = FileSystem.get(conf);
        if(fs.exists(new Path(DIR)))
            fs.delete(new Path(DIR), true);

        if(!calibrateParams(conf)){
            fs.close();
            System.exit(-1);
        }

        if (!createBloomFilters(conf)) {
            fs.close();
            System.exit(-1);
        }
    }

    private static boolean calibrateParams(Configuration conf) throws IOException, InterruptedException, ClassNotFoundException {

        Job job = Job.getInstance(conf, "calibrate");
        job.setJarByClass(Driver.class);

        job.setMapperClass(ParameterCalibration.PCMapper.class);
        job.setReducerClass(ParameterCalibration.PCReducer.class);

        // mapper's output key and output value
        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(IntWritable.class);

        // reducer's output key and output value
        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(Text.class);

        job.setNumReduceTasks(Integer.parseInt(conf.get("numReducer")));
        job.getConfiguration().setDouble("p", Double.parseDouble(conf.get("p")));

        FileInputFormat.addInputPath(job, new Path(conf.get("input.path"))); //input file that needs to be used by MapReduce program
        FileOutputFormat.setOutputPath(job, new Path(conf.get("output.path"))); //output file

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        return job.waitForCompletion(true);
    }

    private static boolean createBloomFilters(Configuration conf) throws IOException, InterruptedException, ClassNotFoundException {

        Job job = Job.getInstance(conf, "create");
        job.setJarByClass(BloomFilterCreation.class);

        job.setMapperClass(BloomFilterCreation.BFCMapper.class);
        job.setReducerClass(BloomFilterCreation.BFCReducer.class);

        // mapper's output key and output value
        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(BloomFilter.class);

        // reducer's output key and output value
        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(BloomFilter.class);

        FileInputFormat.addInputPath(job, new Path(conf.get("input.path")));
        FileOutputFormat.setOutputPath(job, new Path(conf.get("output.bloom-filters-creation")));

        return job.waitForCompletion(true);
    }

    private static boolean validateParams(Configuration conf) throws IOException, InterruptedException, ClassNotFoundException {

        Job job = Job.getInstance(conf, "validate");
        job.setJarByClass(ParameterValidation.class);

        job.setMapperClass(ParameterValidation.PVMapper.class);
        job.setReducerClass(ParameterValidation.PVReducer.class);

        // mapper's output key and output value
        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(BloomFilter.class);

        // reducer's output key and output value
        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(BloomFilter.class);

        FileInputFormat.addInputPath(job, new Path(conf.get("output.bloom-filters-creation")));
        FileOutputFormat.setOutputPath(job, new Path(conf.get("output.parameter-validation")));

        return job.waitForCompletion(true);
    }

}