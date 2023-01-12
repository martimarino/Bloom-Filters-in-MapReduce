package main.java.it.unipi.cc.calibration;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class CalibrationMapper extends Mapper<Object, Text, IntWritable, IntWritable> {

    private static int n_rates;     // number of bloom filters
    private static int[] counter;   // how many film for every bloom filter

    private static final IntWritable outputKey = new IntWritable();
    private static final IntWritable outputVal = new IntWritable();

    @Override
    protected void setup(Context context) {
        n_rates = Integer.parseInt(context.getConfiguration().get("n_rates"));
        counter = new int[n_rates];
    }

    @Override
    public void map(Object key, Text value, Context context) {

        String record = value.toString();
        if (record == null || record.startsWith("tconst"))
            return;
        String[] tokens = record.split("\t");

        int rate = Math.round(Float.parseFloat(tokens[1])); // token : <title, rating, numVotes>
        counter[rate - 1]++;
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {

        for (int i = 0; i < n_rates; i++) {
            outputKey.set(i + 1);
            outputVal.set(counter[i]);
            context.write(outputKey, outputVal);        // emit mapper results
        }
    }
}