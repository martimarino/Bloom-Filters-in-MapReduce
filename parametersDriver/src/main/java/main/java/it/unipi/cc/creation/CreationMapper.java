package main.java.it.unipi.cc.creation;

import main.java.it.unipi.cc.model.IntArrayWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.util.hash.MurmurHash;

import java.io.IOException;

import static org.apache.hadoop.util.hash.Hash.MURMUR_HASH;

public class CreationMapper extends Mapper<Object, Text, IntWritable, IntArrayWritable> {

    private static int k;                                               // k value (the same for all bloom filters)
    private static IntWritable[] indices;                               // bit positions to set
    private static final IntWritable outputKey = new IntWritable();
    private static final IntArrayWritable outputValue = new IntArrayWritable();

    @Override
    protected void setup(Context context) {
        k = Integer.parseInt(context.getConfiguration().get("filter_k"));
        indices = new IntWritable[k];
    }

    @Override
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {

        if(value.toString().startsWith("tconst"))                       // skip the header
            return;
        String[] tokens = value.toString().split("\t");           // token : <title, rating, numVotes>
        int roundedRating = (int) Math.round(Double.parseDouble(tokens[1]));
        int m = Integer.parseInt(context.getConfiguration().get("filter_" + roundedRating + "_m"));

        // compute the k hash functions
        for(int i = 0; i < k; i++)
            indices[i] = new IntWritable(Math.abs(MurmurHash.getInstance(MURMUR_HASH).hash(tokens[0].getBytes(), i) % m));

        outputKey.set(roundedRating);
        outputValue.set(indices);
        context.write(outputKey, outputValue);
    }
}