from airflow.contrib.hooks.ssh_hook import SSHHook
from airflow.contrib.operators.ssh_execute_operator import SSHExecuteOperator
from airflow import DAG
from datetime import datetime, timedelta
from qubole_templete import *

spark_process_dimension = spark_job_template.format(
    mainClass="com.thoughtworks.pipeline.DimensionTransformation",
    args=s3_bucket)
spark_process_facts = spark_job_template.format(
    mainClass="com.thoughtworks.pipeline.FactTransformation",
    args=s3_bucket)

import_nation = import_cmd_template.format(table="nation")
import_region = import_cmd_template.format(table="region")
import_customer = import_cmd_template.format(table="customer")
import_lineitem = import_cmd_template.format(table="lineitem")
import_orders = import_cmd_template.format(table="orders")
import_part = import_cmd_template.format(table="part")
import_supplier = import_cmd_template.format(table="supplier")

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

dag = DAG('qubole_data_pipeline', default_args=default_args, schedule_interval=timedelta(days=1))

connection = "qubole_connection"
sshHook = SSHHook(conn_id=connection)

task_import_nation = SSHExecuteOperator(
    task_id="import_nation",
    bash_command=import_nation,
    ssh_hook=sshHook,
    dag=dag)

task_import_region = SSHExecuteOperator(
    task_id="import_region",
    bash_command=import_region,
    ssh_hook=sshHook,
    dag=dag)
task_import_customer = SSHExecuteOperator(
    task_id="import_customer",
    bash_command=import_customer,
    ssh_hook=sshHook,
    dag=dag)
task_import_supplier = SSHExecuteOperator(
    task_id="import_supplier",
    bash_command=import_supplier,
    ssh_hook=sshHook,
    dag=dag)
task_import_orders = SSHExecuteOperator(
    task_id="import_orders",
    bash_command=import_orders,
    ssh_hook=sshHook,
    dag=dag)
task_import_part = SSHExecuteOperator(
    task_id="import_part",
    bash_command=import_part,
    ssh_hook=sshHook,
    dag=dag)
task_import_lineitem = SSHExecuteOperator(
    task_id="import_lineitem",
    bash_command=import_lineitem,
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

task2.set_upstream(task_or_task_list=[task_import_lineitem, task_import_orders, task_import_part,
                                      task_import_region, task_import_supplier, task_import_customer,
                                      task_import_nation])
task3.set_upstream(task2)

task3.set_downstream(task_or_task_list=[task4, task5, task6, task7, task8, task9])
