package it.unipi.cc.creation;

import it.unipi.cc.model.BloomFilter;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import java.io.IOException;
import java.util.ArrayList;

public class CreationMapper extends Mapper<Object, Text, IntWritable, BloomFilter> {

    private static ArrayList<BloomFilter> bloomFilters = new ArrayList<>();
    private static int n_rates; //number of rates

    private static final IntWritable outputKey = new IntWritable();

    //creation of the 10 blooom filters basing on corresponding m,k
    public void setup(Context context) {

        n_rates = Integer.parseInt(context.getConfiguration().get("n_rates"));
        int k = Integer.parseInt(context.getConfiguration().get("filter_k")); //need to define after

        for (int i = 0; i < n_rates; i++) {
            int m = Integer.parseInt(context.getConfiguration().get("filter_" + (i+1) + "_m")); //need to define after
            bloomFilters.add(i, new BloomFilter(m,k));
        }

    }
    //insert into bloomfilter corresponding to the calculated rating, the id of the film, for each film
    public void map(Object key, Text value, Context context){
        if(value.toString().startsWith("tconst"))                       // skip the header
            return;
        String[] tokens = value.toString().split("\t"); //id (0) , rating (1)
        int roundedRating = (int) Math.round(Double.parseDouble(tokens[1]))-1;
        bloomFilters.get(roundedRating).insert(tokens[0]);
    }
    public void cleanup(Context context) throws IOException, InterruptedException {

        for(int i = 0; i < n_rates; i++) {
            outputKey.set(i + 1);
            context.write(outputKey, bloomFilters.get(i));
        }
    }
}