package datalake.ri.datapipeline.workflow.actions;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;

public class FlightDelayPipeline {
    protected static JavaSparkContext javaSparkContext;
    protected static String inputFileName;
    protected static String outputFileName;
    protected static String user;

    protected static void initialize(String[] args) {
        if (args.length != 3) {
            System.out.println("Invalid Length of Arguments");
            System.exit(1);
        }
        SparkConf sparkConf = new SparkConf().setAppName("Datalake Pipeline");
        javaSparkContext = new JavaSparkContext(sparkConf);
        user = args[0];
        inputFileName = args[1];
        outputFileName = args[2];
    }
}
