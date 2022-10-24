package it.unipi.cc.hadoop.mapreduce;

import it.unipi.cc.hadoop.model.BloomFilter;
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

public class BloomFilterFPR {
    private static int n_rates;
    private static final IntWritable outputKey = new IntWritable();
    private static final IntWritable outputVal = new IntWritable();
    //private static int[] d_counters;

    public static class FPRMapper extends Mapper<Object, Text, IntWritable, IntWritable>{
        private static int[] fp_counters;
        private final ArrayList<BloomFilter> bloomFilters = new ArrayList<>();

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            super.setup(context);
            n_rates = context.getConfiguration().getInt("n_rates", 0);
            if(n_rates == 0)
                System.exit(-1);

            fp_counters = new int[n_rates];
            //d_counters = new int[n_rates];
            for(int i=0; i<n_rates; i++)
                bloomFilters.add(new BloomFilter());

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
            if (record == null || record.startsWith("tconst"))
                return;
            String[] tokens = record.split("\t");  //id (0) , rating (1)
            int roundedRating = (int) Math.round(Double.parseDouble(tokens[1]))-1;

            for(int i = 0; i < n_rates; i++) {
                if (roundedRating == i)
                    continue;

                //d_counters[i]++;
                if (bloomFilters.get(i).find(tokens[0]))
                    fp_counters[i]++;
            }
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            for(int i = 0; i < n_rates; i++) {
                outputKey.set(i+1);
                outputKey.set(fp_counters[i]);
                context.write(outputKey, outputVal);
            }
        }
    }

    public static class FPRReducer extends Reducer<IntWritable, IntWritable, IntWritable, IntWritable> {
        private final int[] counter = new int[n_rates];

        // rate and mappers count in input
        @Override
        public void reduce(IntWritable key, Iterable<IntWritable> mapper_counts, Context context) {
            for (IntWritable value : mapper_counts)
                counter[key.get()-1] += value.get();
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            for (int i = 0; i < n_rates; i++){
                outputKey.set(i+1);
                outputVal.set(counter[i]);
                //System.out.println("Rate\t\tFP\t\tCount\t\tFPR");
                //System.out.println("--------------------------------------------------------------------------------");
                //System.out.println(outputKey+"\t\t\t"+counter[i]+"\t\t\t"+d_counters[i]+"\t\t\t"+(counter[i]/d_counters[i]));
                context.write(outputKey, outputVal);
            }
        }
    }

}
