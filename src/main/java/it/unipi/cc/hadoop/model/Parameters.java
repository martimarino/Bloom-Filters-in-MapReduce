package it.unipi.cc.hadoop.model;

import java.io.FileInputStream;
import java.util.Properties;

public class Parameters {
    private String inputPath;
    private String outputPath;
    private double p;

    // Hadoop
    private int numReducer;

    public Parameters(String path) {
        Properties prop = new Properties();

        FileInputStream fis = null;
        try {
            fis = new FileInputStream(path);
            prop.load(fis);
        } catch (Exception e) {
            e.printStackTrace();
            System.exit(1);
        }

        inputPath = prop.getProperty("inputPath");
        outputPath = prop.getProperty("outputPath");
        p = Double.parseDouble(prop.getProperty("p"));
    }

    public void setInputPath(String inputPath) {
        this.inputPath = inputPath;
    }
    public String getInputPath() { return inputPath; }
    public void setOutputPath(String outputPath) {
        this.outputPath = outputPath;
    }
    public String getOutputPath() {
        return outputPath;
    }
    public void setP(double p) {
        this.p = p;
    }
    public double getP() {
        return p;
    }
    public void setNumReducer(int numReducer) { this.numReducer = numReducer; }
    public int getNumReducer() { return numReducer; }

}
