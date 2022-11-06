package it.unipi.cc.mapreduce;

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

public class BloomFilterFP {
    private static int n_rates;
    private static final IntWritable outputKey = new IntWritable();
    private static final IntWritable outputValue = new IntWritable();

    public static class FPMapper extends Mapper<Object, Text, IntWritable, IntWritable>{
        private static int[] fp_counters;
        private final ArrayList<BloomFilter> bloomFilters = new ArrayList<>();

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            super.setup(context);
            n_rates = context.getConfiguration().getInt("n_rates", 0);
            if(n_rates == 0)
                System.exit(-1);

            fp_counters = new int[n_rates];

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
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {

            String record = value.toString();
            if (record == null || record.startsWith("tconst"))
                return;
            String[] tokens = record.split("\t");  //id (0) , rating (1)
            int roundedRating = (int) Math.round(Double.parseDouble(tokens[1]))-1;

            for(int i = 0; i < n_rates; i++) {
                if (roundedRating == i)
                    continue;

                if (bloomFilters.get(i).find(tokens[0]))
                    fp_counters[i]++;

                outputKey.set(i+1);
                outputValue.set(fp_counters[i]);
                context.write(outputKey, outputValue);
            }
        }
    }

    public static class FPReducer extends Reducer<IntWritable, IntWritable, IntWritable, IntWritable> {
        private static int counter;

        // rate and mappers count in input
        @Override
        public void reduce(IntWritable key, Iterable<IntWritable> mapper_counts, Context context) throws IOException, InterruptedException {

            counter = 0;

            for (IntWritable value : mapper_counts)
                counter += value.get();

            //ESECUZIONE IN LOCALE
//            try {
//                BufferedWriter out = new BufferedWriter(new FileWriter("fpr.txt", true));
//                out.write("RATE " + key.get() + "\tCOUNTER: " + counter + "\n");
//                out.close();
//            } catch (IOException e) {
//                System.out.println("exception occurred" + e);
//            }

            //ESECUZIONE SU CLUSTER
            FileSystem fs = FileSystem.get(context.getConfiguration());
            Path filenamePath = new Path("output/FP" + key.get() + ".txt");
            try {
                FSDataOutputStream fin = fs.create(filenamePath);
                fin.writeUTF("key: " + key.get() + '\n');
                fin.writeUTF("counter: " + counter + "\n");
                fin.close();
            } catch (Exception e){
                e.printStackTrace();
            }

            outputKey.set(key.get());
            outputValue.set(counter);
            context.write(outputKey, outputValue); //write rate and number of fp
        }
    }

}
