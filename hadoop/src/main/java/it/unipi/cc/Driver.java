package it.unipi.cc;

import it.unipi.cc.mapreduce.BloomFilterCreation;
import it.unipi.cc.mapreduce.BloomFilterFP;
import it.unipi.cc.model.BloomFilter;
import it.unipi.cc.model.IntArrayWritable;
import it.unipi.cc.mapreduce.ParameterCalibration;
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
    private static final String OUTPUT_FP = "fp";

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
        System.out.println("----------------------------------------------------");
        System.out.println("Configuration variables\n");
        System.out.println("Input\t\t\t" + otherArgs[0]);
        System.out.println("Number of rates:\t" + otherArgs[1]);
        System.out.println("Number of reducers:\t" + otherArgs[2]);
        System.out.println("P:\t\t\t" + otherArgs[3]);
        System.out.println("Number of lines:\t" + otherArgs[4]);
        System.out.println("----------------------------------------------------\n");

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

        // read output of first stage and add m, k to configuration
        FileStatus[] status = fs.listStatus(new Path(OUTPUT_FOLDER + OUTPUT_CALIBRATE));
        int[] params = new int[3];              // for m, k, n
        int[] rate_count = new int[N_RATES];    // array of n of every bloom filter

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
                    IntWritable nWritable = (IntWritable) value.get()[2];
                    params[0] = mWritable.get();
                    params[1] = kWritable.get();
                    params[2] = nWritable.get();
                    conf.set("filter_k", String.valueOf(params[1]));
                    conf.set("filter_" + key.get()+ "_m", String.valueOf(params[0]));
                    rate_count[key.get()-1] = params[2];
                    Driver.print("Rating " + key + "\tm: " + params[0] + "\tk: " + params[1] + "\tn: " + params[2]);
                }
            }
        }

        // second stage
        print("Bloom filters creation stage...");
        if (!createBloomFilters(conf)) {
            fs.close();
            System.exit(-1);
        }
        print("BloomFilters correctly created!");

        // third stage
        print("FP computation stage...");
        conf.set("outStage2", OUTPUT_FOLDER+OUTPUT_CREATE);
        if (!computeFP(conf)) {
            fs.close();
            System.exit(-1);
        }
        print("FP correctly computed!");

        // get fp from 3rd stage output and compute fpr for every rating
        status = fs.listStatus(new Path(OUTPUT_FOLDER + OUTPUT_FP));
        int[] falsePositiveCounter = new int[N_RATES];
        for(FileStatus filestatus : status) {
            String f = String.valueOf(filestatus.getPath());
            if(f.contains("SUCCESS"))
                continue;
            IntWritable key = new IntWritable();
            IntWritable value = new IntWritable();
            try (SequenceFile.Reader reader = new SequenceFile.Reader(conf, SequenceFile.Reader.file(filestatus.getPath()))) {
                while (reader.next(key, value)) {
                    falsePositiveCounter[key.get()-1] = value.get();
                }
            }
        }
        double[] falsePositiveRate = new double[N_RATES];
        double tot = 0;

        for (int i=0; i<N_RATES; i++)
            tot += rate_count[i];

        for (int i=0; i<N_RATES; i++) {
            falsePositiveRate[i] = falsePositiveCounter[i]/(tot-rate_count[i]);     // fpr = fp / (tot - tp)
            print("Rating "+(i+1)+"\tfp: "+falsePositiveCounter[i]+"\tfpr: "+String.format("%.4f", falsePositiveRate[i]));
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
        job.setOutputValueClass(IntArrayWritable.class);
        job.setNumReduceTasks(N_REDUCERS);
        job.getConfiguration().setDouble("p", P);

        return configJob(job, OUTPUT_CALIBRATE);
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

        return configJob(job, OUTPUT_CREATE);
    }

    private static boolean computeFP(Configuration conf) throws IOException, InterruptedException, ClassNotFoundException {

        Job job = Job.getInstance(conf, "fp");
        job.setJarByClass(BloomFilterFP.class);

        job.setMapperClass(BloomFilterFP.FPMapper.class);
        job.setReducerClass(BloomFilterFP.FPReducer.class);

        // mapper's output key and output value
        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(IntWritable.class);

        // reducer's output key and output value
        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(IntWritable.class);

        return configJob(job, OUTPUT_FP);
    }

    private static boolean configJob(Job job, String outputPath) throws IOException, InterruptedException, ClassNotFoundException {

        NLineInputFormat.addInputPath(job, new Path(INPUT));
        job.getConfiguration().setInt("mapreduce.input.lineinputformat.linespermap", N_LINES);

        job.setInputFormatClass(NLineInputFormat.class);
        job.setOutputFormatClass(SequenceFileOutputFormat.class);

        FileOutputFormat.setOutputPath(job, new Path(OUTPUT_FOLDER + outputPath));

        return job.waitForCompletion(true);
    }

    // utility function
    public static void print(String s) {
        System.out.println("\n----------------------------------------------------");
        System.out.println(s);
        System.out.println("----------------------------------------------------\n");
    }

}