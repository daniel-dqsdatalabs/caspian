from warnings import filterwarnings
from datetime import datetime

from airflow import DAG
from airflow.contrib.operators.qubole_operator import QuboleOperator

filterwarnings("ignore")

default_args = {
    'owner': 'caspian',
    'depends_on_past': False,
    'start_date': datetime(2016, 06, 06),
}

dag = DAG('airline_pipeline', default_args=default_args)

recordGenerationTask = QuboleOperator(task_id='record_generation',
                                      command_type="sparkcmd",
                                      cmdline="/usr/lib/spark/bin/spark-submit --jars 's3://twi-analytics-sandbox/dev-workspaces/rahul/dependent-jars/prep-buddy-1.0-SNAPSHOT.jar' --class datalake.ri.datapipeline.workflow.actions.FlightDelayRecordGenerator 's3://twi-analytics-sandbox/dev-workspaces/rahul/pipeline-jar/data-lake-1.0-SNAPSHOT.jar' 2007.csv 2007_delay_record",
                                      dag=dag)

recordNormalizationTask = QuboleOperator(task_id='record_normalization',
                                         command_type="sparkcmd",
                                         cmdline="/usr/lib/spark/bin/spark-submit --jars 's3://twi-analytics-sandbox/dev-workspaces/rahul/dependent-jars/prep-buddy-1.0-SNAPSHOT.jar' --class datalake.ri.datapipeline.workflow.actions.FlightDelayRecordNormalizer 's3://twi-analytics-sandbox/dev-workspaces/rahul/pipeline-jar/data-lake-1.0-SNAPSHOT.jar' 2007_delay_record 2007_normalized_record",
                                         dag=dag)

delayPredictionModelGenerationTask = QuboleOperator(task_id='prediction_model',
                                                    command_type="sparkcmd",
                                                    cmdline="/usr/lib/spark/bin/spark-submit --jars 's3://twi-analytics-sandbox/dev-workspaces/rahul/dependent-jars/prep-buddy-1.0-SNAPSHOT.jar' --class datalake.ri.datapipeline.workflow.actions.PredictionModelGenerator 's3://twi-analytics-sandbox/dev-workspaces/rahul/pipeline-jar/data-lake-1.0-SNAPSHOT.jar' 2007_normalized_record delayPredictionModel",
                                                    dag=dag)

testDelayPredictionModel = QuboleOperator(task_id='prediction_model_test',
                                                    command_type="sparkcmd",
                                                    cmdline="/usr/lib/spark/bin/spark-submit --jars 's3://twi-analytics-sandbox/dev-workspaces/rahul/dependent-jars/prep-buddy-1.0-SNAPSHOT.jar' --class datalake.ri.datapipeline.workflow.actions.PredictionModelTester 's3://twi-analytics-sandbox/dev-workspaces/rahul/pipeline-jar/data-lake-1.0-SNAPSHOT.jar' delayPredictionModel",
                                                    dag=dag)

recordGenerationTask.set_downstream(recordNormalizationTask)
recordNormalizationTask.set_downstream(delayPredictionModelGenerationTask)
delayPredictionModelGenerationTask.set_downstream(testDelayPredictionModel)