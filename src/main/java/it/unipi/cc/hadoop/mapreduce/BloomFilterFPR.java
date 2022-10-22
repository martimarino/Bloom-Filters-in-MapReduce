package it.unipi.cc.hadoop.mapreduce;

import it.unipi.cc.hadoop.Driver;
import it.unipi.cc.hadoop.model.BloomFilter;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.ArrayList;

public class BloomFilterFPR {
    private static int n_rates;
    private static final IntWritable outputKey = new IntWritable();
    private static final IntWritable outputVal = new IntWritable();

    public static class FPRMapper extends Mapper<IntWritable, IntWritable, IntWritable, IntWritable>{
        private static int[] fp_counters;
        private ArrayList<BloomFilter> bloomFilters = new ArrayList<>();

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            super.setup(context);
            Driver.print("FPR -> setup");
            n_rates = context.getConfiguration().getInt("n_rates", 0);
            if(n_rates == 0)
                System.exit(-1);

            fp_counters = new int[n_rates];
            for(int i=0; i<n_rates; i++)
                bloomFilters.set(i, new BloomFilter());

            FileSystem fs = FileSystem.get(context.getConfiguration());
            FileStatus[] status = fs.listStatus(new Path("output/outStage2"));

            for(FileStatus filestatus : status) {
                String f = String.valueOf(filestatus.getPath());
                System.out.println(f);
                if(f.contains("_SUCCESS") || f.contains("part"))
                    continue;

                SequenceFile.Reader reader = new SequenceFile.Reader(context.getConfiguration(), SequenceFile.Reader.file(new Path(f)));

                IntWritable key = new IntWritable();
                BloomFilter bf = new BloomFilter();
                reader.next(key, bf);
                bloomFilters.set(key.get()-1, bf);
            }
        }

        @Override
        public void map(IntWritable film, IntWritable value, Context context) {
            Driver.print("FPR -> map");
            String[] split = value.toString().split("\t");  //id (0) , rating (1)
            int roundedRating = (int) Math.round(Double.parseDouble(split[1]))-1;
            for(int i = 0; i < n_rates; i++) {
                if (roundedRating == i)
                    continue;
                if (bloomFilters.get(i).find(split[0]))
                    fp_counters[i]++;
            }
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            Driver.print("FPR -> mapper cleanup");
            for(int i = 0; i < n_rates; i++) {
                outputKey.set(i+1);
                outputVal.set(fp_counters[i]);
                context.write(outputKey, outputKey);
            }
        }

    }

    public static class FPRReducer extends Reducer<IntWritable, IntWritable, IntWritable, IntWritable> {
        private int sum = 0;

        // rate and mappers count in input
        @Override
        public void reduce(IntWritable key, Iterable<IntWritable> mapper_counts, Context context) {
            Driver.print("FPR -> reduce");
            for (IntWritable value : mapper_counts)
                sum += value.get();
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            Driver.print("FPR -> reducer cleanup");
            for (int i = 0; i < n_rates; i++){
                outputKey.set(i+1);
                outputVal.set(sum);
                context.write(outputKey, outputKey);
            }
        }

    }

}
