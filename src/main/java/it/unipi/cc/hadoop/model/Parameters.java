package it.unipi.cc.hadoop.model;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Properties;

public class Parameters {
    private String inputPath;
    private String outputPath;
    private double p;

    // Hadoop
    private int nReducersJob0;
    private int nReducersJob1;
    private int nReducersJob2;
    private int nLineSplitJob1;
    private boolean verbose;

    public Parameters(String path) {
        Properties prop = new Properties();

        FileInputStream fis = null;
        try {
            fis = new FileInputStream(path);
            prop.load(fis);
        } catch (FileNotFoundException e) {
            e.printStackTrace();
            System.exit(1);
        } catch (IOException e) {
            e.printStackTrace();
            System.exit(1);
        }

        inputPath = prop.getProperty("inputPath");
        outputPath = prop.getProperty("outputPath");
        p = Double.parseDouble(prop.getProperty("p"));
    }

    public String getInputPath() {
        return inputPath;
    }

    public void setInputPath(String inputPath) {
        this.inputPath = inputPath;
    }

    public String getOutputPath() {
        return outputPath;
    }

    public void setOutputPath(String outputPath) {
        this.outputPath = outputPath;
    }

    public double getP() {
        return p;
    }

    public void setP(double p) {
        this.p = p;
    }
}
