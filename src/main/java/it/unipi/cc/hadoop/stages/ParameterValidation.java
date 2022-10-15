package it.unipi.cc.hadoop.stages;

import it.unipi.cc.hadoop.BloomFilter;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.ArrayList;

public class ParameterValidation {

    private static int n_rates;
    private static int[] fp_counters;
    private static final IntWritable outputKey = new IntWritable();
    private static final IntWritable outputVal = new IntWritable();

    public class PVMapper extends Mapper<IntWritable, IntWritable, IntWritable, IntWritable>{

        private final ArrayList<BloomFilter> bloomFilters = new ArrayList<>();

        @Override
        public void setup(Context context) throws IOException, InterruptedException {

            super.setup(context);
            n_rates = context.getConfiguration().getInt("n_rates", 0);
            if(n_rates == 0)
                System.exit(-1);
            fp_counters = new int[n_rates];
            for(int i = 0; i < n_rates; i++) {
                bloomFilters.set(i, new BloomFilter());
            }
        }

        @Override
        public void map(IntWritable film, IntWritable value, Context context) throws IOException, InterruptedException {
            super.map(film, value, context);
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
        public void cleanup(Context context) throws IOException, InterruptedException {
            for(int i = 0; i < n_rates; i++) {
                outputKey.set(i+1);
                outputVal.set(fp_counters[i]);
                context.write(outputKey, outputKey);
            }
        }

    }

    public class PVReducer extends Reducer<IntWritable, IntWritable, IntWritable, IntWritable> {

        private int sum = 0;

        // rate and mappers count in input
        @Override
        public void reduce(IntWritable key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {

            for (IntWritable value : values)
                sum += value.get();
        }

        @Override
        public void cleanup(Context context) throws IOException, InterruptedException {
            for (int i = 0; i < n_rates; i++){
                outputKey.set(i+1);
                outputVal.set(sum);
                context.write(outputKey, outputKey);
            }
        }

    }

}
