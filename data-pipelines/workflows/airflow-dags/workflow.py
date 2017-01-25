from airflow.contrib.hooks.ssh_hook import SSHHook
from airflow.contrib.operators.ssh_execute_operator import SSHExecuteOperator
from airflow import DAG
from datetime import datetime, timedelta
from templete import s3_bucket, export_cmd_template, spark_job_template, import_all_cmd_template

sync_jars = "aws s3 sync " + s3_bucket + "jars/ ."
import_all_tables = import_all_cmd_template.format(exclude_tables="partsupp")

spark_process_dimension = spark_job_template.format(
    mainClass="com.thoughtworks.pipeline.DimensionTransformation",
    args=s3_bucket)
spark_process_facts = spark_job_template.format(
    mainClass="com.thoughtworks.pipeline.FactTransformation",
    args=s3_bucket)

export_dimNation = export_cmd_template.format(table="nation_dim", inputPath="dimensions/dimNation")
export_dimCustomer = export_cmd_template.format(table="customer_dim", inputPath="dimensions/dimCustomer")
export_dimSupplier = export_cmd_template.format(table="supplier_dim", inputPath="dimensions/dimSupplier")
export_dimPart = export_cmd_template.format(table="part_dim", inputPath="dimensions/dimPart")
export_dimDate = export_cmd_template.format(table="date_dim", inputPath="dimensions/dimDate")
export_salesFact = export_cmd_template.format(table="sales_fact", inputPath="facts/factSales")

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
    task_id="import_all_tables",
    bash_command=import_all_tables,
    ssh_hook=sshHook,
    dag=dag)

task2 = SSHExecuteOperator(
    task_id="spark_process_dimension",
    bash_command=spark_process_dimension,
    ssh_hook=sshHook,
    dag=dag)

task3 = SSHExecuteOperator(
    task_id="spark_process_facts",
    bash_command=spark_process_facts,
    ssh_hook=sshHook,
    dag=dag)

task4 = SSHExecuteOperator(
    task_id="export_dimNation",
    bash_command=export_dimNation,
    ssh_hook=sshHook,
    dag=dag)

task5 = SSHExecuteOperator(
    task_id="export_dimCustomer",
    bash_command=export_dimCustomer,
    ssh_hook=sshHook,
    dag=dag)

task6 = SSHExecuteOperator(
    task_id="export_dimSupplier",
    bash_command=export_dimSupplier,
    ssh_hook=sshHook,
    dag=dag)

task7 = SSHExecuteOperator(
    task_id="export_dimPart",
    bash_command=export_dimPart,
    ssh_hook=sshHook,
    dag=dag)

task8 = SSHExecuteOperator(
    task_id="export_dimDate",
    bash_command=export_dimDate,
    ssh_hook=sshHook,
    dag=dag)

task9 = SSHExecuteOperator(
    task_id="export_salesFact",
    bash_command=export_salesFact,
    ssh_hook=sshHook,
    dag=dag)

task0.set_downstream(task1)
task1.set_downstream(task2)
task3.set_upstream(task2)
task3.set_downstream(task_or_task_list=[task4, task5, task6, task7, task8, task9])
