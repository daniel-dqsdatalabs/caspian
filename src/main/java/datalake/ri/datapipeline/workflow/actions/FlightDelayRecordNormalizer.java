package datalake.ri.datapipeline.workflow.actions;

import org.apache.prepbuddy.datacleansers.RowPurger;
import org.apache.prepbuddy.normalizers.ZScoreNormalization;
import org.apache.prepbuddy.rdds.TransformableRDD;
import org.apache.prepbuddy.typesystem.FileType;
import org.apache.prepbuddy.utils.RowRecord;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;

public class FlightDelayRecordNormalizer {
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

    public static void main(String[] args) {
        initialize(args);
        TransformableRDD travelledFlights = new TransformableRDD(sc.textFile("s3://twi-analytics-sandbox/dev-workspaces/rahul/data/input/" + inputFileName));

        JavaRDD<String> normalizedData = travelledFlights
                .deduplicate()
                .removeRows(new RowPurger.Predicate() {
                    @Override
                    public Boolean evaluate(RowRecord rowRecord) {
                        boolean hasNA = false;
                        for (int index = 0; index < 6; index++)
                            hasNA = hasNA || rowRecord.valueAt(index).trim().equals("NA");
                        return hasNA;
                    }
                })
                .normalize(0, new ZScoreNormalization())
                .normalize(1, new ZScoreNormalization())
                .normalize(2, new ZScoreNormalization())
                .normalize(3, new ZScoreNormalization())
                .normalize(4, new ZScoreNormalization())
                .map(new Function<String, String>() {
                    @Override
                    public String call(String record) throws Exception {
                        String[] recordAsArray = FileType.CSV.parseRecord(record);
                        double arrivalDelay = Double.parseDouble(recordAsArray[5]);
                        String label = arrivalDelay >= 10 ? "1" : "0";
                        recordAsArray[5] = label;
                        return FileType.CSV.join(recordAsArray);
                    }
                });

        normalizedData.saveAsTextFile("s3://twi-analytics-sandbox/dev-workspaces/rahul/data/normalized/" + outputFileName);
    }
}
