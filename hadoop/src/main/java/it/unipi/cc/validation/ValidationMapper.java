package it.unipi.cc.validation;

import it.unipi.cc.model.BloomFilter;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.ArrayList;

public class ValidationMapper extends Mapper<Object, Text, IntWritable, IntWritable> {

    private static int n_rates;
    private static final IntWritable outputKey = new IntWritable();
    private static final IntWritable outputValue = new IntWritable();

    private static int[] fp_counters;
    private final ArrayList<BloomFilter> bloomFilters = new ArrayList<>();

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {

        n_rates = context.getConfiguration().getInt("n_rates", 0);
        if(n_rates == 0)
            System.exit(-1);

        fp_counters = new int[n_rates];

        for(int i=0; i<n_rates; i++)
            bloomFilters.add(new BloomFilter());

        // read bloom filters from creation stage and reconstruct them
        FileSystem fs = FileSystem.get(context.getConfiguration());
        FileStatus[] status = fs.listStatus(new Path(context.getConfiguration().get("outStage2")));

        for(FileStatus filestatus : status) {
            String f = String.valueOf(filestatus.getPath());
            if(f.contains("_SUCCESS") || f.contains("part"))
                continue;

            IntWritable key;
            BloomFilter bf;
            try (SequenceFile.Reader reader = new SequenceFile.Reader(context.getConfiguration(), SequenceFile.Reader.file(new Path(f)))) {
                key = new IntWritable();
                bf = new BloomFilter();
                reader.next(key, bf);
            }
            bloomFilters.set(key.get()-1, bf);
        }
    }

    @Override
    public void map(Object key, Text value, Context context) {

        String record = value.toString();
        if (record == null || record.startsWith("tconst"))      // skip the header
            return;
        String[] tokens = record.split("\t");             // token : <title, rating, numVotes>
        int roundedRating = (int) Math.round(Double.parseDouble(tokens[1]))-1;

        for(int i = 0; i < n_rates; i++) {
            if (roundedRating == i)         // skip the true positive
                continue;

            if (bloomFilters.get(i).find(tokens[0]))        // count the false positives
                fp_counters[i]++;

        }
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        for(int i = 0; i < n_rates; i++) {
            outputKey.set(i+1);
            outputValue.set(fp_counters[i]);
            context.write(outputKey, outputValue);
        }
    }
}