import airflow
from airflow import DAG
from airflow.operators import BashOperator,PythonOperator
from datetime import datetime, timedelta
from airflow.contrib.operators.dataflow_operator import DataFlowPythonOperator
from parameters.job_parameters import *
#from __future__ import absolute_import


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': airflow.utils.dates.days_ago(2),
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=10),
}

dag = DAG('run_DF_file_params',
  default_args=default_args,
  schedule_interval=timedelta(days=1)
  )
#add files to DAG directory
t1 = BashOperator(
  task_id='copy_files',
  bash_command='gsutil -m cp gs://totemic-splicer-272114-df-test/DF_Jobs/*.py /home/airflow/gcs/data/',
  dag=dag)



t2= DataFlowPythonOperator(
task_id= 'dataflow_runner',
py_file = '/home/airflow/gcs/data/main.py',
gcp_conn_id='google_cloud_default',
dataflow_default_options=dataflow,
options=other_options,
dag=dag
)




t1 >> t2
