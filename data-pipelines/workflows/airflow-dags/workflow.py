from airflow.contrib.hooks.ssh_hook import SSHHook
from airflow.contrib.operators.ssh_execute_operator import SSHExecuteOperator
from airflow import DAG
from datetime import datetime, timedelta
from templete import s3_bucket, import_cmd_template, export_cmd_template, spark_job_template

sync_jars = "aws s3 sync " + s3_bucket + "jars/ ."
import_nation = import_cmd_template.format(table="nation")
import_region = import_cmd_template.format(table="region")
spark_transformation = spark_job_template.format(mainClass="NationDimension", args=s3_bucket)
export_nation = export_cmd_template.format(table="nation_dim", inputPath="deNormalizedNationTable")

default_args = {
    'owner': 'thoughtworks',
    'depends_on_past': False,
    'start_date': datetime(2017, 1, 13),
    'email': ['airflow@airflow.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=4),
}

dag = DAG('data_pipeline', default_args=default_args, schedule_interval=timedelta(days=1))

connection = "hadoop_connection"
sshHook = SSHHook(conn_id=connection)

task0 = SSHExecuteOperator(
    task_id="sync_jars",
    bash_command=sync_jars,
    ssh_hook=sshHook,
    dag=dag)

task1 = SSHExecuteOperator(
    task_id="import_nation",
    bash_command=import_nation,
    ssh_hook=sshHook,
    dag=dag)

task2 = SSHExecuteOperator(
    task_id="import_region",
    bash_command=import_region,
    ssh_hook=sshHook,
    dag=dag)

task3 = SSHExecuteOperator(
    task_id="spark_job",
    bash_command=spark_transformation,
    ssh_hook=sshHook,
    dag=dag)

task4 = SSHExecuteOperator(
    task_id="export_nation",
    bash_command=export_nation,
    ssh_hook=sshHook,
    dag=dag)

task0.set_downstream(task1)
task1.set_downstream(task2)
task2.set_downstream(task3)
task3.set_downstream(task4)
