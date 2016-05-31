package datalake.ri.datapipeline.workflow.actions;

import org.apache.prepbuddy.typesystem.FileType;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.mllib.classification.LogisticRegressionModel;
import org.apache.spark.mllib.classification.LogisticRegressionWithSGD;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.mllib.linalg.Vectors;
import org.apache.spark.mllib.regression.LabeledPoint;
import scala.Tuple2;

public class PredictionModelGenerator extends FlightDelayPipeline {
    public static void main(String[] args) {
        initialize(args);
        JavaRDD<String> initialNormalizedData = javaSparkContext.textFile("s3://twi-analytics-sandbox/dev-workspaces/" + user + "/data/normalized/" + inputFileName);
        JavaRDD<LabeledPoint> trainingData = initialNormalizedData.map(new Function<String, LabeledPoint>() {
            @Override
            public LabeledPoint call(String record) throws Exception {
                String[] recordAsArray = FileType.CSV.parseRecord(record);
                double arrivalDelay = Double.parseDouble(recordAsArray[5]);
                return new LabeledPoint(arrivalDelay, generateDenseVector(recordAsArray));
            }
        }).cache();

        LogisticRegressionModel logisticRegressionModel = LogisticRegressionWithSGD.train(trainingData.rdd(), 100);
        testModel(logisticRegressionModel);

        logisticRegressionModel.save(javaSparkContext.sc(), "s3://twi-analytics-sandbox/dev-workspaces/" + user + "/data/predictionModel/" + outputFileName);
    }

    private static void testModel(final LogisticRegressionModel logisticRegressionModel) {
        JavaRDD<String> testDataset = javaSparkContext.textFile("s3://twi-analytics-sandbox/dev-workspaces/" + user + "/data/normalized/2008_normalized_record");
        JavaRDD<Tuple2<Double,Double>> labelWithPrediction = testDataset.map(new Function<String, Tuple2<Double,Double>>() {
            @Override
            public Tuple2<Double, Double> call(String record) throws Exception {
                String[] recordAsArray = FileType.CSV.parseRecord(record);
                double actualDelayStatus = Double.parseDouble(recordAsArray[5]);
                double predictedDelayStatus = logisticRegressionModel.predict(generateDenseVector(recordAsArray));
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
