package it.unipi.cc;

import it.unipi.cc.mapreduce.BloomFilterCreation;
import it.unipi.cc.mapreduce.BloomFilterFP;
import it.unipi.cc.mapreduce.ParameterCalibration;
import it.unipi.cc.model.BloomFilter;
import it.unipi.cc.model.IntArrayWritable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.NLineInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import java.io.IOException;

public class Driver {

    // files for input and output
    private static final String OUTPUT_FOLDER = "output/";
    private static final String OUTPUT_CALIBRATE = "outStage1";
    private static final String OUTPUT_CREATE = "outStage2";
    private static final String OUTPUT_FP = "fp_rates";

    // configuration variables
    private static String INPUT;
    private static int N_RATES;
    private static int N_REDUCERS;
    private static double P;
    private static int N_LINES;


    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {

        Configuration conf = new Configuration();

        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        if(otherArgs.length != 5) {
            print("Arguments required: <input> <n_rates> <n_reducers> <p> <n_lines>");
            System.exit(-1);
        }

        //print configuration
        System.out.println("---------------------------------------");
        System.out.println("Configuration variables\n");
        System.out.println("---------------------------------------");
        System.out.println("Input\t\t\t\t" + otherArgs[0]);
        System.out.println("Number of rates:\t" + otherArgs[1]);
        System.out.println("Number of reducers:\t" + otherArgs[2]);
        System.out.println("P:\t\t\t\t\t" + otherArgs[3]);
        System.out.println("Number of lines:\t" + otherArgs[4]);
        System.out.println("---------------------------------------\n");

        //get args from terminal
        INPUT = otherArgs[0];
        N_RATES = Integer.parseInt(otherArgs[1]);
        conf.set("n_rates", otherArgs[1]);
        N_REDUCERS = Integer.parseInt(otherArgs[2]);
        P = Double.parseDouble(otherArgs[3]);
        N_LINES = Integer.parseInt(otherArgs[4]);

        //errors
        if(P < 0 || P > 1 || N_RATES <= 0 || N_REDUCERS <= 0 || INPUT.equals("")) {
            print("Configuration not valid: check the arguments passed.");
            System.exit(-1);
        }

        // Clean HDFS workspace
        print("Clean HDFS workspace");
        FileSystem fs = FileSystem.get(conf);
        if(fs.exists(new Path(OUTPUT_FOLDER)))
            fs.delete(new Path(OUTPUT_FOLDER), true);

        // first stage
        print("Parameter calibration stage...");
        if(!calibrateParams(conf)){
            fs.close();
            System.exit(-1);
        }
        print("Parameters correctly calibrated!");

        //set m and k parameters
        FileStatus[] status = fs.listStatus(new Path(OUTPUT_FOLDER + OUTPUT_CALIBRATE));
        int[] params = new int[2];

        for(FileStatus filestatus : status) {
            String f = String.valueOf(filestatus.getPath());
            if(f.contains("SUCCESS"))
                continue;
            IntWritable key = new IntWritable();
            IntArrayWritable value = new IntArrayWritable();
            try (SequenceFile.Reader reader = new SequenceFile.Reader(conf, SequenceFile.Reader.file(filestatus.getPath()))) {
                while (reader.next(key, value)) {
                    IntWritable mWritable = (IntWritable) value.get()[0];
                    IntWritable kWritable = (IntWritable) value.get()[1];
                    params[0] = mWritable.get();
                    params[1] = kWritable.get();
                    conf.set("filter_k", String.valueOf(params[1]));
                    conf.set("filter_" + key.get()+ "_m", String.valueOf(params[0]));
                }
            }
        }

        //second stage
        print("Bloom filters creation stage...");
        if (!createBloomFilters(conf)) {
            fs.close();
            System.exit(-1);
        }
        print("BloomFilters correctly created!");

        //third stage
        print("FP computation stage...");
        conf.set("outStage2", OUTPUT_FOLDER+OUTPUT_CREATE);
        if (!computeFP(conf)) {
            fs.close();
            System.exit(-1);
        }
        print("FP correctly computed!");

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
        job.setOutputValueClass(IntArrayWritable.class);
        job.setNumReduceTasks(N_REDUCERS);
        job.getConfiguration().setDouble("p", P);

        NLineInputFormat.addInputPath(job, new Path(INPUT));
        job.getConfiguration().setInt("mapreduce.input.lineinputformat.linespermap", N_LINES);

        FileOutputFormat.setOutputPath(job, new Path(OUTPUT_FOLDER+OUTPUT_CALIBRATE)); //output file

        job.setInputFormatClass(NLineInputFormat.class);
        job.setOutputFormatClass(SequenceFileOutputFormat.class);

        return job.waitForCompletion(true);
    }

    private static boolean createBloomFilters(Configuration conf) throws IOException, InterruptedException, ClassNotFoundException {

        Job job = Job.getInstance(conf, "create");
        job.setJarByClass(BloomFilterCreation.class);

        job.setMapperClass(BloomFilterCreation.BFCMapper.class);
        job.setReducerClass(BloomFilterCreation.BFCReducer.class);

        // mapper's output key and output value
        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(IntArrayWritable.class);

        // reducer's output key and output value
        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(BloomFilter.class);

        NLineInputFormat.addInputPath(job, new Path(INPUT));
        job.getConfiguration().setInt("mapreduce.input.lineinputformat.linespermap", N_LINES);

        job.setInputFormatClass(NLineInputFormat.class);
        job.setOutputFormatClass(SequenceFileOutputFormat.class);

        FileOutputFormat.setOutputPath(job, new Path(OUTPUT_FOLDER+OUTPUT_CREATE));

        return job.waitForCompletion(true);
    }

    private static boolean computeFP(Configuration conf) throws IOException, InterruptedException, ClassNotFoundException {

        Job job = Job.getInstance(conf, "fpr");
        job.setJarByClass(BloomFilterFP.class);

        job.setMapperClass(BloomFilterFP.FPMapper.class);
        job.setReducerClass(BloomFilterFP.FPReducer.class);

        // mapper's output key and output value
        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(IntWritable.class);

        // reducer's output key and output value
        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(IntWritable.class);

        NLineInputFormat.addInputPath(job, new Path(INPUT));
        job.getConfiguration().setInt("mapreduce.input.lineinputformat.linespermap", N_LINES);

        job.setInputFormatClass(NLineInputFormat.class);
        job.setOutputFormatClass(SequenceFileOutputFormat.class);

        FileOutputFormat.setOutputPath(job, new Path(OUTPUT_FOLDER+OUTPUT_FP));

        return job.waitForCompletion(true);
    }

    public static void print(String s) {
        System.out.println("\n---------------------------------------");
        System.out.println(s);
        System.out.println("---------------------------------------\n");
    }

}