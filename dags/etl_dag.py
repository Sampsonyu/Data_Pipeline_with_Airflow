from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators import (StageToRedshiftOperator, LoadFactOperator,
                                LoadDimensionOperator, DataQualityOperator)

from airflow.operators.postgres_operator import PostgresOperator
from airflow.contrib.hooks.aws_hook import AwsHook
from helpers import SqlQueries

"""
This ETL pipeline is for:
1. Inserting fact and dimension tables from S3 bucket, 
2. Filling the our Amazon Redshift RDB
3. Running checks on the data for verification.
"""

# AWS_KEY = os.environ.get('AWS_KEY')
# AWS_SECRET = os.environ.get('AWS_SECRET')
# aws_hook = AwsHook("aws_credentials")
# aws_credentials = aws_hook.get_credentials()



default_args = {
    'owner': 'Sampson',
    'start_date': datetime.now(),
    'catchup': False,
    'depends_on_past': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5)
}

dag = DAG('etl_dag',
          default_args=default_args,
          description='Load and transform data in Redshift with Airflow',
          schedule_interval='@hourly'  # or alternatively '0 * * * *'
        )

start_operator = DummyOperator(task_id='Begin_execution',  dag=dag)

stage_events_to_redshift = StageToRedshiftOperator(
    task_id='Stage_events',
    dag=dag,
    redshift_conn_id="redshift",
    aws_credentials_id="aws_credentials",
    s3_path= 's3://sparkifydatasource/udacity-dend/log_data',
    table='staging_events',
    s3_json_option='s3://sparkifydatasource/udacity-dend/log_json_path.json',      
)

stage_songs_to_redshift = StageToRedshiftOperator(
    task_id='Stage_songs',
    dag=dag,
    redshift_conn_id="redshift",
    aws_credentials_id="aws_credentials",
    s3_path= 's3://sparkifydatasource/udacity-dend/song_data',
    table='staging_songs',
    s3_json_option='auto' 
)
# s3_json_options="FORMAT AS JSON 'auto'" 
load_songplays_table = LoadFactOperator(
    task_id='Load_songplays_fact_table',
    dag=dag,
    redshift_conn_id="redshift",
    table='songplays',
    sql=SqlQueries.songplay_table_insert
)

load_user_dimension_table = LoadDimensionOperator(
    task_id='Load_user_dim_table',
    dag=dag,
    redshift_conn_id="redshift",
    table="users",
    sql=SqlQueries.user_table_insert,
    truncate=True
)

load_song_dimension_table = LoadDimensionOperator(
    task_id='Load_song_dim_table',
    dag=dag,
    redshift_conn_id="redshift",
    table="songs",
    sql=SqlQueries.song_table_insert,
    truncate=True
)

load_artist_dimension_table = LoadDimensionOperator(
    task_id='Load_artist_dim_table',
    dag=dag,
    redshift_conn_id="redshift",
    table="artists",
    sql=SqlQueries.artist_table_insert,
    truncate=True
)

load_time_dimension_table = LoadDimensionOperator(
    task_id='Load_time_dim_table',
    dag=dag,
    redshift_conn_id="redshift",
    table="time",
    sql=SqlQueries.time_table_insert,
    truncate=True
)

dq_checks=[
{'check_sql':'SELECT COUNT(*) FROM songplays WHERE userid IS NULL' , 'expected_result':'result==0', 'check_info': 'NULL value check on userid column in songplays table'},
{'check_sql':'SELECT COUNT(*) FROM artists WHERE name IS NULL' , 'expected_result':'result==0', 'check_info': 'NULL value check on name column in artists table'}
]

run_quality_checks = DataQualityOperator(
    task_id='Run_data_quality_checks',
    dag=dag,
    redshift_conn_id = 'redshift',
    table_list=['songplays', 'songs', 'artists', 'users', 'public.time'],
    quality_checks = dq_checks
)


end_operator = DummyOperator(task_id='End_execution',  dag=dag)


# Configure the task dependencies such that the graph looks like the following:
#
#                                                                      -> load_song_dimension_table -
#                -> stage_events_to_redshift                          /                              \
#               /                            \                       /-> load_user_dimension_table ---\
#start_operator                               -> load_songplays_table                                  -> run_quality_checks -> end_operator
#               \                            /                       \-> load_artist_dimension_table -/
#                -> stage_songs_to_redshift                           \                              /
#                                                                      -> load_time_dimension_table /


# Graph definition
start_operator >> [stage_events_to_redshift, stage_songs_to_redshift]
[stage_events_to_redshift, stage_songs_to_redshift] >> load_songplays_table 
load_songplays_table >> [load_user_dimension_table, 
                         load_song_dimension_table, 
                         load_artist_dimension_table,
                         load_time_dimension_table]
[load_user_dimension_table,
 load_song_dimension_table,
 load_artist_dimension_table,
 load_time_dimension_table] >> run_quality_checks
run_quality_checks >> end_operator