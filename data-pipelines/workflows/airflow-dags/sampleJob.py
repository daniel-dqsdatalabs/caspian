from airflow import DAG
from datetime import datetime
from airflow.contrib.operators.qubole_operator import QuboleOperator

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2015, 6, 3),
    'email': ['airflow@airflow.com'],
    'email_on_failure': False,
    'email_on_retry': False
}


dag = DAG('example_qubole_operator', default_args=default_args)

task1 = QuboleOperator(task_id='spark_cmd', command_type="sparkcmd",
                          cmdline="/usr/lib/spark/bin/spark-submit --jars 's3://twi-analytics-sandbox/dev-workspaces/rahul/dependent-jars/prep-buddy-1.0-SNAPSHOT.jar' --class datalake.ri.datapipeline.workflow.actions.FlightDelayRecordGenerator 's3://twi-analytics-sandbox/dev-workspaces/lalit/pipeline-jar/data-lake-1.0-SNAPSHOT.jar' 2008.csv 2008_delay_record",
                          tags='aiflow_example_run', dag=dag)
