package it.unipi.cc.validation;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class ValidationReducer extends Reducer<IntWritable, IntWritable, IntWritable, IntWritable> {

    private static final IntWritable outputKey = new IntWritable();
    private static final IntWritable outputValue = new IntWritable();

    // rating and mappers count in input
    @Override
    public void reduce(IntWritable key, Iterable<IntWritable> mapper_counts, Context context) throws IOException, InterruptedException {

        int counter = 0;        // sum of all the false positives relative to the key

        for (IntWritable value : mapper_counts)
            counter += value.get();

        //ESECUZIONE SU CLUSTER
//        FileSystem fs = FileSystem.get(context.getConfiguration());
//        Path filenamePath = new Path("output/FP" + key.get() + ".txt");
//        try {
//            FSDataOutputStream fin = fs.create(filenamePath);
//            fin.writeUTF("key: " + key.get() + '\n');
//            fin.writeUTF("counter: " + counter + "\n");
//            fin.close();
//        } catch (Exception e){
//            e.printStackTrace();
//        }

        outputKey.set(key.get());
        outputValue.set(counter);
        context.write(outputKey, outputValue); // write rating and number of fp
    }
}
