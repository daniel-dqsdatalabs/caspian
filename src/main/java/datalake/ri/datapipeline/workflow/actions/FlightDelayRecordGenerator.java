package datalake.ri.datapipeline.workflow.actions;

import org.apache.prepbuddy.rdds.TransformableRDD;
import org.apache.prepbuddy.typesystem.FileType;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;

import java.io.IOException;

public class FlightDelayRecordGenerator {

    private static JavaSparkContext sc;
    private static String inputFileName;
    private static String outputFileName;

    private static void initialize(String[] args) {
        if (args.length != 2) {
            System.out.println("Invalid Length of Arguments");
            System.exit(1);
        }

        SparkConf sparkConf = new SparkConf().setAppName("FlightDelayRecordGenerator");
        sc = new JavaSparkContext(sparkConf);
        inputFileName = args[0];
        outputFileName = args[1];
    }

    public static void main(String[] args) throws IOException {
        initialize(args);
        JavaRDD<String> _1987FlightTransactionDetails = sc.textFile("s3://twi-analytics-sandbox/very-large-datasets/airline-operations/" + inputFileName);

        JavaRDD<String> flightsThatAreNotCancelled = _1987FlightTransactionDetails.filter(new Function<String, Boolean>() {
            public Boolean call(String record) throws Exception {
                String[] recordAsArray = FileType.CSV.parseRecord(record);
                return recordAsArray[21].trim().equals("0");
            }
        });

        JavaRDD<String> dimensionallyReducedFlightData = flightsThatAreNotCancelled.map(new Function<String, String>() {
            @Override
            public String call(String record) throws Exception {
                String[] recordAsArray = FileType.CSV.parseRecord(record);

                String month = recordAsArray[1];
                String dayOfMonth = recordAsArray[2];
                String dayOfWeek = recordAsArray[3];
                String scheduledDepartureTime = recordAsArray[5];
                String arrivalDelay = recordAsArray[14];
                String distance = recordAsArray[18];

                String[] reducedRow = new String[]{month, dayOfMonth, dayOfWeek, scheduledDepartureTime, distance, arrivalDelay};
                return FileType.CSV.join(reducedRow);
            }
        });

        new TransformableRDD(dimensionallyReducedFlightData).deduplicate().saveAsTextFile("s3://twi-analytics-sandbox/dev-workspaces/rahul/data/input/" + outputFileName);
    }
}
