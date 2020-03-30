import airflow
from airflow import DAG
from airflow.operators import BashOperator,PythonOperator
from datetime import datetime, timedelta
from airflow.contrib.operators.dataflow_operator import DataFlowPythonOperator
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

dag = DAG('run_DF_file',
  default_args=default_args,
  schedule_interval=timedelta(days=1)
  )
t1 = BashOperator(
  task_id='copy_files',
  bash_command='gsutil -m cp gs://totemic-splicer-272114-df-test/DF_Jobs/*.py /home/airflow/gcs/data/',
  dag=dag)



t3= DataFlowPythonOperator(
task_id= 'dataflow_runner',
py_file = '/home/airflow/gcs/data/main.py',
gcp_conn_id='google_cloud_default',
dataflow_default_options={
    "project": "totemic-splicer-272114",
    "job_name": 'my_job',
    "temp_location": "gs://totemic-splicer-272114-df-test/temp",
    "staging_location": "gs://totemic-splicer-272114-df-test/staging"
    },
options={
    'input': "gs://totemic-splicer-272114-df-test/Input_Files/tf_test.txt",
    'output': "gs://totemic-splicer-272114-df-test/Output_Files/tf_test.txt"
    },
dag=dag
)


t1 >> t3
