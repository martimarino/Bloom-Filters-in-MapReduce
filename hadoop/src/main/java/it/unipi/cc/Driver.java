package it.unipi.cc;

import it.unipi.cc.mapreduce.BloomFilterCreation;
import it.unipi.cc.mapreduce.BloomFilterFPR;
import it.unipi.cc.mapreduce.ParameterCalibration;
import it.unipi.cc.model.BloomFilter;
import org.apache.hadoop.mapreduce.lib.input.NLineInputFormat;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;

public class Driver {

    // files for input and output
    private static final String OUTPUT_FOLDER = "output/";
    private static final String OUTPUT_CALIBRATE = "outStage1";
    private static final String OUTPUT_CREATE = "outStage2";
    private static final String OUTPUT_FPR = "fp_rates";

    // configuration variables
    private static String INPUT;
    private static int N_RATES;
    private static int N_REDUCERS;
    private static double P;
    private static int N_LINES;

    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {

        Configuration conf = new Configuration();

/*        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        if(otherArgs.length != 5) {
            print("Arguments required: <input> <n_rates> <n_reducers> <p> <n_lines>");
            System.exit(-1);
        }*/
        String[] otherArgs = {"dataset.tsv", "10", "1", "0.01", "800000"};

        System.out.println("---------------------------------------");
        System.out.println("Configuration variables\n");
        System.out.println("---------------------------------------");
        System.out.println("Input\t\t\t" + otherArgs[0]);
        System.out.println("Number of rates:\t" + otherArgs[1]);
        System.out.println("Number of reducers:\t" + otherArgs[2]);
        System.out.println("P:\t\t\t" + otherArgs[3]);
        System.out.println("Number of lines:\t\t" + otherArgs[4]);
        System.out.println("---------------------------------------\n");


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

        print("Parameter calibration stage...");
        // first stage
        if(!calibrateParams(conf)){
            fs.close();
            System.exit(-1);
        }
        print("FASE 1 TERMINATA");

        FileStatus[] status = fs.listStatus(new Path(OUTPUT_FOLDER + OUTPUT_CALIBRATE));
        List<String> param = new ArrayList<>();

        for(FileStatus filestatus : status) {
            BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(filestatus.getPath())));
            for(String line = br.readLine(); line != null; line = br.readLine()) {
                String[] sp = line.split("\t");
                int m = Integer.parseInt(sp[1]);
                int k = Integer.parseInt(sp[2]);
                param.add(m + " " + k);
            }
            br.close();
        }

        for(int i = 0; i < N_RATES; i++) {
            String[] token = param.get(i).split(" ");
            if(i == 0)
                conf.set("filter_k", token[1]);
            conf.set("filter_" + (i+1) + "_m", token[0]);
        }

        print("Bloom filters creation stage...");
        if (!createBloomFilters(conf)) {
            fs.close();
            System.exit(-1);
        }
        print("FASE 2 TERMINATA");

        print("FPR computation stage...");
        conf.set("outStage2", OUTPUT_FOLDER+OUTPUT_CREATE);
        if (!computeFPR(conf)) {
            fs.close();
            System.exit(-1);
        }

        print("The End");

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
        job.setNumReduceTasks(N_REDUCERS);
        job.getConfiguration().setDouble("p", P);

        NLineInputFormat.addInputPath(job, new Path(INPUT));
        job.getConfiguration().setInt("mapreduce.input.lineinputformat.linespermap", N_LINES);

        FileOutputFormat.setOutputPath(job, new Path(OUTPUT_FOLDER+OUTPUT_CALIBRATE)); //output file

        job.setInputFormatClass(NLineInputFormat.class);
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
        job.setMapOutputValueClass(IntWritable.class);

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

    private static boolean computeFPR(Configuration conf) throws IOException, InterruptedException, ClassNotFoundException {

        Job job = Job.getInstance(conf, "fpr");
        job.setJarByClass(BloomFilterFPR.class);

        job.setMapperClass(BloomFilterFPR.FPRMapper.class);
        job.setReducerClass(BloomFilterFPR.FPRReducer.class);

        // mapper's output key and output value
        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(IntWritable.class);

        // reducer's output key and output value
        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(IntWritable.class);

        NLineInputFormat.addInputPath(job, new Path(INPUT));
        job.getConfiguration().setInt("mapreduce.input.lineinputformat.linespermap", N_LINES);

        FileOutputFormat.setOutputPath(job, new Path(OUTPUT_FOLDER+OUTPUT_FPR));

        return job.waitForCompletion(true);
    }

    public static void print(String s) {
        System.out.println("\n---------------------------------------");
        System.out.println(s);
        System.out.println("---------------------------------------\n");
    }

}