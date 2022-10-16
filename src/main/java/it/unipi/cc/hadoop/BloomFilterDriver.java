package it.unipi.cc.hadoop;

import it.unipi.cc.hadoop.model.Parameters;
import it.unipi.cc.hadoop.mapreduce.ParameterCalibration;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;

import java.io.IOException;

public class BloomFilterDriver {

    private static String INPUT_PATH;
    private static int NUM_REDUCERS;

    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
        Configuration conf = new Configuration();



        Parameters parameters = new Parameters("/Users/luanabussu/GitHub/Bloom-Filters-in-MapReduce/conf.properties");

        String DIR = parameters.getOutputPath()+"/";
        conf.set("input.path", parameters.getInputPath());
        conf.set("output.path", parameters.getOutputPath());
        conf.setDouble("p", parameters.getP());

        conf.set("output.parameter-calibration", DIR + "parameter-calibration");
        conf.set("output.bloom-filters", DIR + "bloom-filters");
        conf.set("output.parameter-validation", DIR + "parameter-validation");

        // Clean HDFS workspace
        FileSystem fs = FileSystem.get(conf);
        fs.delete(new Path(DIR), true);

        Job parameterCalibration = Job.getInstance(conf, "ParameterCalibration");
        if(!ParameterCalibration.main(parameterCalibration)){
            fs.close();
            System.exit(1);
        }
    }
}
