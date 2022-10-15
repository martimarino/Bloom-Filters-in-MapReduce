package it.unipi.cc.hadoop;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.GenericOptionsParser;

public class Driver {

    private static String INPUT_PATH;
    private static int NUM_REDUCERS;

    public static void main(String[] args) throws Exception {

        Configuration conf = new Configuration();

        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        if (otherArgs.length != 5){
            System.out.println("ERROR: " + "<data> <p> <output_file> <number reducers> <lines>");
            System.exit(-1);
        }

        System.out.println("[Configurations]");
        System.out.println("data ->\t"+otherArgs[0]);
        System.out.println("p ->\t"+otherArgs[1]);
        System.out.println("output file ->\t"+otherArgs[2]);
        System.out.println("number reducers ->\t"+otherArgs[3]);
        System.out.println("lines ->\t"+otherArgs[4]);

        if (NUM_REDUCERS < 1){
            System.err.println("ERROR: " + "Reducers number too low");
            System.exit(-1);
        }
    }

}
