package main.java.it.unipi.cc.calibration;

import main.java.it.unipi.cc.model.IntArrayWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;


public class CalibrationReducer extends Reducer<IntWritable, IntWritable, IntWritable, IntWritable> {

    private static double p;
    private static IntWritable outputValue = new IntWritable();

    @Override
    protected void setup(Context context) {
        p = context.getConfiguration().getDouble("p", 0.01);
    }

    @Override
    public void reduce(IntWritable key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {

        int n = 0;      // sum of all counters relative to the key
        while(values.iterator().hasNext())
            n += values.iterator().next().get();

        //Driver.print("RATE:" + key.get() + " M: " + m + " K: " + k + "N: " + n);
        outputValue= new IntWritable(n);

        context.write(key, outputValue);     // n for every bloom filter
    }
}