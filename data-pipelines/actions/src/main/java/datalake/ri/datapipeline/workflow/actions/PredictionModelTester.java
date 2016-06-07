package datalake.ri.datapipeline.workflow.actions;

import org.apache.prepbuddy.typesystem.FileType;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.mllib.classification.LogisticRegressionModel;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.mllib.linalg.Vectors;
import scala.Tuple2;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

public class PredictionModelTester {
    protected static JavaSparkContext javaSparkContext;
    protected static String inputFileName;
    protected static String user;

    protected static void initialize(String[] args) {
        if (args.length != 1) {
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
    }

    public static void main(String[] args) {
        initialize(args);
        final LogisticRegressionModel delayPredictionModel = LogisticRegressionModel.load(javaSparkContext.sc(), "s3://twi-analytics-sandbox/dev-workspaces/" + user + "/data/predictionModel/" + inputFileName);

        JavaRDD<String> normalizedTestDatasetForPredictionModel = javaSparkContext.textFile("s3://twi-analytics-sandbox/dev-workspaces/" + user + "/data/normalized/2008_normalized_record");
        JavaRDD<Tuple2<Double, Double>> labelWithPrediction = normalizedTestDatasetForPredictionModel.map(new Function<String, Tuple2<Double, Double>>() {
            @Override
            public Tuple2<Double, Double> call(String record) throws Exception {
                String[] recordAsArray = FileType.CSV.parseRecord(record);
                double actualDelayStatus = Double.parseDouble(recordAsArray[5]);
                double predictedDelayStatus = delayPredictionModel.predict(generateDenseVector(recordAsArray));
                return new Tuple2<Double, Double>(predictedDelayStatus, actualDelayStatus);
            }
        });
        printAccuracySummary(labelWithPrediction);
    }

    private static void printAccuracySummary(JavaRDD<Tuple2<Double, Double>> labelWithPrediction) {
        double correctDelayPrediction = labelWithPrediction.filter(new Function<Tuple2<Double, Double>, Boolean>() {
            @Override
            public Boolean call(Tuple2<Double, Double> predictionAndActualValue) throws Exception {
                return predictionAndActualValue._1() == 1 && predictionAndActualValue._2() == 1;
            }
        }).count();
        double correctNonDelayPrediction = labelWithPrediction.filter(new Function<Tuple2<Double, Double>, Boolean>() {
            @Override
            public Boolean call(Tuple2<Double, Double> predictionAndActualValue) throws Exception {
                return predictionAndActualValue._1() == 0 && predictionAndActualValue._2() == 0;
            }
        }).count();
        double incorrectNonDelayPrediction = labelWithPrediction.filter(new Function<Tuple2<Double, Double>, Boolean>() {
            @Override
            public Boolean call(Tuple2<Double, Double> predictionAndActualValue) throws Exception {
                return predictionAndActualValue._1() == 1 && predictionAndActualValue._2() == 0;
            }
        }).count();
        double incorrectDelayPrediction = labelWithPrediction.filter(new Function<Tuple2<Double, Double>, Boolean>() {
            @Override
            public Boolean call(Tuple2<Double, Double> predictionAndActualValue) throws Exception {
                return predictionAndActualValue._1() == 0 && predictionAndActualValue._2() == 1;
            }
        }).count();

        double precision = correctDelayPrediction / (correctDelayPrediction + incorrectNonDelayPrediction);
        double recall = correctDelayPrediction / (correctDelayPrediction + incorrectDelayPrediction);
        double F_measure = 2 * precision * recall / (precision + recall);
        double accuracy = (correctDelayPrediction + correctNonDelayPrediction) / (correctDelayPrediction + correctNonDelayPrediction + incorrectNonDelayPrediction + incorrectDelayPrediction);

        System.out.printf("Precision : %s\nRecall : %s\nF_measure : %s\nAccuracy : %s", precision, recall, F_measure, accuracy);
    }

    private static Vector generateDenseVector(String[] record) {
        double month = Double.parseDouble(record[0]);
        double dayOfMonth = Double.parseDouble(record[1]);
        double dayOfWeek = Double.parseDouble(record[2]);
        double scheduledDepartureTime = Double.parseDouble(record[3]);
        double distance = Double.parseDouble(record[4]);
        return Vectors.dense(month, dayOfMonth, dayOfWeek, scheduledDepartureTime, distance);
    }
}
