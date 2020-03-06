import airflow
from airflow import DAG
from datetime import timedelta
from airflow.models import Variable
from airflow.operators.bash_operator import BashOperator
from airflow.operators.http_operator import SimpleHttpOperator
from airflow.operators.sensors import HttpSensor
import json



default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': airflow.utils.dates.days_ago(2),
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG('dataprep_regional_analysis_2',
          default_args=default_args,
          schedule_interval=timedelta(days=1),
          )

start = BashOperator(task_id='start_message',
                     bash_command='echo "started"',
                     dag=dag)

output_id = Variable.get("output_id")
region = Variable.get("region")
headers = {
    "Content-Type": "application/json",
    "Authorization": Variable.get("trifacta_bearer")
    }

def run_flow_and_wait_for_completion():
    run_flow_task = SimpleHttpOperator(
        http_conn_id='dataprep',
        method='POST',
        task_id='run_flow',
        endpoint='/v4/jobGroups',
        data=json.dumps({"wrangledDataset": {"id": int(output_id)},"runParameters": {"overrides": {"data": [{"key": "country","value": region}]}}}),
        headers=headers,
        xcom_push=True,
        dag=dag,
      )


run_flow_and_wait_for_completion()
