package datalake.ri.datapipeline.workflow.actions;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

public class FlightDelayPipeline {
    protected static JavaSparkContext javaSparkContext;
    protected static String inputFileName;
    protected static String outputFileName;
    protected static String user;

    protected static void initialize(String[] args) {
        if (args.length != 2) {
            System.out.println("Invalid Length of Arguments");
            System.exit(1);
        }
        try {
            InputStream stream = FlightDelayPipeline.class.getResourceAsStream("/user/user.properties");
            Properties properties = new Properties();
            properties.load(stream);
            user = properties.getProperty("user");
        } catch (IOException e) {
            System.out.println("Can not load User File in user/user.properties in your jar");
            System.exit(1);
        }
        System.out.println("Executing the command for the user:--" + user);
        SparkConf sparkConf = new SparkConf().setAppName("DataLake Pipeline");
        javaSparkContext = new JavaSparkContext(sparkConf);
        inputFileName = args[0];
        outputFileName = args[1];
    }
}
