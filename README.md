# Data_Pipeline_with_Airflow

## Project Description

A music streaming company, Sparkify, has decided that it is time to introduce more automation and monitoring to their data warehouse ETL pipelines and come to the conclusion that the best tool to achieve this is Apache Airflow.


In this project we created high-grade data pipelines that are dynamic and built from reusable tasks, can be monitored, and allow easy backfills. a data quality operator is implemented to report volume discrepancies in real-time.

The source datasets consist of JSON logs that tell about user activity in the application and JSON metadata about the songs the users listen to.

## Data Pipeline

The goal of this project is to author a data pipeline workflow created with custom operators within the Airflow platform that perform tasks such as staging data, populating the data warehouse, and running quality checks. A high-level implementation of the pipeline is
![ERD image](/images/dag_graph_view.PNG) as illustrated below.

## Datasets
Here are the s3 links for datasets used in this project:

`Log data: s3://udacity-dend/log_data`
`Song data: s3://udacity-dend/song_data`

## Project Structure
```
Data-Pipeline-with-Airflow
│   README.md                    # Project description
└───airflow                      # Airflow home
|   |
│   └───dags                             # Airflow DAGs location
│   |   └─── etl_dag.py  # DAG definition
|   └───plugins
|   |   └─── helpers
|   |   |   └─── sql_queries.py     # All sql queries required
|   |   └───operators
|   |       └─── data_quality.py    # DataQualityOperator
|   |       └─── load_dimension.py  # LoadDimensionOperator
|   |       └─── load_fact.py       # LoadFactOperator
|   |       └─── s3_to_redshift.py  # S3ToRedshiftOperator
|   └─── crate_table.sql
└───images
    └─── dag_graph_view.png # DAG Graph View
```


Project has two directories named `dags` and `plugins`. A create tables script and readme file are at root level:
- `create_tables.sql`: SQL create table statements provided with template.

`dags` directory contains:
- `sparkify_etl_dag.py`: Defines main DAG, tasks and link the tasks in required order.

`plugins/operators` directory contains:
- `stage_redshift.py`: Defines `StageToRedshiftOperator` to copy JSON data from S3 to staging tables in the Redshift via `copy` command.
- `load_dimension.py`: Defines `LoadDimensionOperator` to load a dimension table from staging table(s).
- `load_fact.py`: Defines `LoadFactOperator` to load fact table from staging table(s).
- `data_quality.py`: Defines `DataQualityOperator` to run data quality checks on all tables passed as parameter.
- `sql_queries.py`: Contains SQL queries for the ETL pipeline (provided in template).

## Configuration

This code uses `python 3` and assumes that Apache Airflow is installed and configured.

For AWS credentials, enter the following values:
<ul>
<li><strong>Conn Id</strong>: Enter  <code>aws_credentials</code>.</li>
<li><strong>Conn Type</strong>: Enter  <code>Amazon Web Services</code>.</li>
<li><strong>Login</strong>: Enter your  <strong>Access key ID</strong>  from the IAM User credentials you downloaded earlier.</li>
<li><strong>Password</strong>: Enter your  <strong>Secret access key</strong>  from the IAM User credentials you downloaded earlier.</li>
</ul>

Use the following values in Airflow’s UI to configure connection to Redshift:
<ul>
<li><strong>Conn Id</strong>: Enter  <code>redshift</code>.</li>
<li><strong>Conn Type</strong>: Enter  <code>Postgres</code>.</li>
<li><strong>Host</strong>: Enter the endpoint of your Redshift cluster, excluding the port at the end. You can find this by selecting your cluster in the  <strong>Clusters</strong>  page of the Amazon Redshift console. See where this is located in the screenshot below. IMPORTANT: Make sure to  <strong>NOT</strong>  include the port at the end of the Redshift endpoint string.</li>
<li><strong>Schema</strong>: Enter  <code>dev</code>. This is the Redshift database you want to connect to.</li>
<li><strong>Login</strong>: Enter  <code>awsuser</code>.</li>
<li><strong>Password</strong>: Enter the password you created when launching your Redshift cluster.</li>
<li><strong>Port</strong>: Enter  <code>5439</code>.</li>


