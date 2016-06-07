package datalake.ri.datapipeline.workflow.actions;

import org.apache.prepbuddy.typesystem.FileType;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.mllib.classification.LogisticRegressionModel;
import org.apache.spark.mllib.classification.LogisticRegressionWithSGD;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.mllib.linalg.Vectors;
import org.apache.spark.mllib.regression.LabeledPoint;

public class PredictionModelGenerator extends FlightDelayPipeline {
    public static void main(String[] args) {
        initialize(args);
        JavaRDD<String> normalizedDelayRecord = javaSparkContext.textFile("s3://twi-analytics-sandbox/dev-workspaces/" + user + "/data/normalized/" + inputFileName);
        JavaRDD<LabeledPoint> labeledTrainingData = normalizedDelayRecord.map(new Function<String, LabeledPoint>() {
            @Override
            public LabeledPoint call(String record) throws Exception {
                String[] recordAsArray = FileType.CSV.parseRecord(record);
                double arrivalDelay = Double.parseDouble(recordAsArray[5]);
                return new LabeledPoint(arrivalDelay, generateDenseVector(recordAsArray));
            }
        }).cache();

        LogisticRegressionModel logisticRegressionModel = LogisticRegressionWithSGD.train(labeledTrainingData.rdd(), 100);
        logisticRegressionModel.save(javaSparkContext.sc(), "s3://twi-analytics-sandbox/dev-workspaces/" + user + "/data/predictionModel/" + outputFileName);
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
