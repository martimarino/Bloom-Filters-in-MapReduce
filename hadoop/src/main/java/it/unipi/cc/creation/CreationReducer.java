package it.unipi.cc.creation;

import it.unipi.cc.model.BloomFilter;
import it.unipi.cc.model.IntArrayWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

import java.io.IOException;
import java.util.BitSet;

public class CreationReducer extends Reducer<IntWritable, IntArrayWritable, IntWritable, BloomFilter> { //<rating, indices, rating, bloom filter

    private static MultipleOutputs mos;
    private static int k;

    @Override
    protected void setup(Context context) {
        mos = new MultipleOutputs(context);
        k = Integer.parseInt(context.getConfiguration().get("filter_k"));
    }

    @Override
    public void reduce(IntWritable key, Iterable<IntArrayWritable> values, Context context) throws IOException, InterruptedException {

        int m = Integer.parseInt(context.getConfiguration().get("filter_" + key + "_m"));

        // create and populate bloom filter
        BitSet bitset = new BitSet(m);
        for(IntArrayWritable arr: values) {
            for (Writable writable : arr.get()) {
                IntWritable intWritable = (IntWritable)writable;
                bitset.set(intWritable.get());
            }
        }
        BloomFilter bloomfilter = new BloomFilter(k, m, bitset);
        mos.write(key, bloomfilter, "rate"+key);

    }
    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        mos.close();
    }
}