package it.unipi.cc.calibration;

import it.unipi.cc.model.IntArrayWritable;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;


public class CalibrationReducer extends Reducer<IntWritable, IntWritable, IntWritable, IntArrayWritable> {

    private static double p;
    private static IntWritable[] arr;    // for m, k, n
    private static final IntArrayWritable params = new IntArrayWritable();

    @Override
    protected void setup(Context context) {
        p = context.getConfiguration().getDouble("p", 0.01);
        arr = new IntWritable[3];
    }

    @Override
    public void reduce(IntWritable key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {

        int n = 0;      // sum of all counters relative to the key
        while(values.iterator().hasNext())
            n += values.iterator().next().get();

        int m = (int) (- (n * Math.log(p)) / (Math.pow(Math.log(2),2)));
        int k = (int) ((m/n) * Math.log(2));

        //Driver.print("RATE:" + key.get() + " M: " + m + " K: " + k + "N: " + n);
        arr[0] = new IntWritable(m);
        arr[1] = new IntWritable(k);
        arr[2] = new IntWritable(n);
        params.set(arr);

        context.write(key, params);     // emit m, k, n for every bloom filter

        //ESECUZIONE SU CLUSTER
        FileSystem fs = FileSystem.get(context.getConfiguration());
        Path filenamePath = new Path("output/nmk" + key.get() + ".txt");
        try {
            FSDataOutputStream fin = fs.create(filenamePath);
            fin.writeUTF("key " + key.get() + '\n');
            fin.writeUTF("m: " + m + '\n');
            fin.writeUTF("k: " + k + '\n');
            fin.writeUTF("n: " + n + '\n');
            fin.close();
        } catch (Exception e){
            e.printStackTrace();
        }

    }
}