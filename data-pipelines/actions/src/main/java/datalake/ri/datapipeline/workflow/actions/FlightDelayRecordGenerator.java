package datalake.ri.datapipeline.workflow.actions;

import org.apache.prepbuddy.rdds.TransformableRDD;
import org.apache.prepbuddy.typesystem.FileType;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;

import java.io.IOException;

public class FlightDelayRecordGenerator extends FlightDelayPipeline {
    public static void main(String[] args) throws IOException {
        initialize(args);
        JavaRDD<String> flightTransactionDetails = javaSparkContext.textFile("s3://twi-analytics-sandbox/very-large-datasets/airline-operations/" + inputFileName);
        TransformableRDD tRDDFlightRecords = new TransformableRDD(flightTransactionDetails);

        TransformableRDD flightsThatAreNotCancelled = tRDDFlightRecords.filter(new Function<String, Boolean>() {
            public Boolean call(String record) throws Exception {
                String[] recordAsArray = FileType.CSV.parseRecord(record);
                return recordAsArray[21].trim().equals("0");
            }
        });

        TransformableRDD dimensionallyReducedFlightData = flightsThatAreNotCancelled.select(1, 2, 3, 5, 14, 18);

        new TransformableRDD(dimensionallyReducedFlightData).deduplicate().saveAsTextFile("s3://twi-analytics-sandbox/dev-workspaces/" + user + "/data/input/" + outputFileName);
    }
}
