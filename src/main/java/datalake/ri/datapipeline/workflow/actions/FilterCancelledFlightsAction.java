package datalake.ri.datapipeline.workflow.actions;

import com.qubole.qds.sdk.java.client.*;
import com.qubole.qds.sdk.java.entities.ResultValue;

public class FilterCancelledFlightsAction {

    public static final String QUBOLE_API_KEY = "TMa4XGq7x417SyepB5LKzBuKZdxEEkgzR3mipcsCHPBz5Tbjqt8nqTY1t3SST3Qz";
    public static final String COMMAND_LINE = "/usr/lib/spark/bin/spark-submit --jars 's3://twi-analytics-sandbox/prep-buddy/jar-files/javallier7.jar' --class org.apache.prepbuddy.EncryptionMain 's3://twi-analytics-sandbox/prep-buddy/jar-files/prep-buddy-1.0-Encryption.jar' 's3://twi-analytics-sandbox/prep-buddy/data/calls.csv' 2 --num-executors 4 --executor-cores 8 --executor-memory 5120M";

    public static void main(String[] args) {
        QdsConfiguration configuration = new DefaultQdsConfiguration(QUBOLE_API_KEY);
        QdsClient client = QdsClientFactory.newClient(configuration);
        client.command().spark().clusterLabel("prep-buddy-cluster-1")
                .name("my-query-id")
                .cmdLine(COMMAND_LINE).invoke();

        ResultLatch resultLatch = new ResultLatch(client, "my-query-id");
        try {
            ResultValue resultValue = resultLatch.awaitResult();
            System.out.println("resultLatch = " + resultValue.getResults());
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

}
