package it.unipi.cc.creation;
import it.unipi.cc.model.BloomFilter;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

import java.io.IOException;

public class CreationReducer extends Reducer<IntWritable, BloomFilter, IntWritable, BloomFilter> { //<rating, indices, rating, bloom filter

    private static BloomFilter result;
    private static MultipleOutputs mos;

    //for all the bloomfilter received, doing the or
    protected void setup(Context context) {
        mos = new MultipleOutputs(context);
    }

    public void reduce(IntWritable key, Iterable<BloomFilter> values, Context context) throws IOException, InterruptedException {
        result = new BloomFilter(values.iterator().next());
        while (values.iterator().hasNext()) {
            result.or(values.iterator().next().getBitSet());
        }
        mos.write(key, result,"rate"+key); //<rating, bloomfilter>
    }
    protected void cleanup(Context context) throws IOException, InterruptedException {
        mos.close();
    }
}