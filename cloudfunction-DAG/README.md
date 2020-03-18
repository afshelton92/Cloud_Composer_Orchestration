# Flow from Composer to Function to BQ

Apache Airflow is an excellent orchestration and scheduling tool, but having to maintain a GCP Cloud Composer environment with the resources to run complex processes can be costly. Additionally, rewriting a script to create a DAG can overcomplicate the code. The outlined approach takes a script that would easily run locally, but instead embeds it in an HTTP trigger Cloud Function. From there, the Airflow DAG only needs to send an HTTP request to the function to run it. 

This example takes the above logic, except the Cloud Function script triggers a BigQuery query. To make the query more dynamic, a dependencies file in the Cloud Composer dag directory passes a table name that the Cloud Function uses in the BQ query.

Steps:
1) Create Function and test locally or in Cloud Shell
2) Deploy Function on Cloud Functions
Note: functions cannot be deployed from Cloud Storage (GCSS) and the function must be named main.py and a requirements.txt file should be included
3) Test the function by manually triggering it in the UI or with an HTTP request (if creating an HTTP trigger function)
Note: ensure that the function accepts any included input parameters and that it returns the expected output. After running the function, check the Innovactions graph and the "Errors in last 7 days" metrics.
4) Create an Airflow "Connection" with the following text: https://[*INSERT PROJECT ID*].cloudfunctions.net/
5) Create the Airflow DAG using the SimpleHttpOperator and include the following parameters:

http_conn_id='[name of "Connection" in step 4]',
method='[HTTP method]',
task_id='[Create DAG task Id]',
endpoint='[function name]',
data=json.dumps([dictionary in imported .py file with the input parameters]),  optional
headers={"Content-Type": "application/json"}
xcom_push=True,
dag=dag

6) Run the DAG and monitor the function for innvocations

![Diagram](Composer_Function_Flow.png)
