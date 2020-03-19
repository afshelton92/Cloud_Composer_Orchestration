# Running BQ queries using Cloud Composer

BigQuery is a poweful analytics tool, capable of storing large amounts of data and running complex queries in seconds. While ad hoc queries and analysis are often run within BQ, teams are always looking for ways to operationalize their queries using other tools. Some use cases might include orchestrating complex operations such as appending new records from one tale to another or simply managing scheduled queries from a central tool. Airflow can address both of these use cases and more.

The following example assumes that enterprise-level datasets are shared to minimize storage costs. Teams have read access to those tables, but all require their own queries to transform the data. One solution is to build views that read data from those shared sources. Then read records from the view and append them to tables in datasets that those individual teams own. One reason for choosing this method is that it leaves all of the complex transformations in the BigQuery Views. 

Step 1) Define the BigQuery View definitions.
Consider how frequently new records need to be appended. If possible, use the WHERE clause in the view definition to simplify the orchestration and keep the view small. Ex: The job needs to run daily, so the View's WHERE clause is "date" = current day, etc.. OR consider using the view's "Last modified date" in the WHERE clause ("date" > "last modified date") 
Step 2) Create Cloud Composer Airflow DAG that uses the BigQueryOperator to SELECT * FROM "the view" and append to the destination table. 
This single DAG can be used for multiple tables using a For Loop. Just make sure that each table has it's own task_id. 


