import logging, datetime
from airflow.operators.dummy_operator import DummyOperator

from airflow import DAG

from airflow.operators.postgres_operator import PostgresOperator
from airflow.contrib.hooks.aws_hook import AwsHook

"""
This ETL pipeline is for creating the 2 staging tables, 1 fact table and 5 dimension tables
"""

aws_hook = AwsHook("aws_credentials")
credentials = aws_hook.get_credentials()


dag = DAG('create_tables_dag',
          start_date=datetime.datetime.now(),
          description='Create tables on RedShift',
          schedule_interval=None
        )

start_operator = DummyOperator(task_id='Begin_execution',  dag=dag)

create_table_staging_events = PostgresOperator(
    task_id='create_table_staging_events',
    dag=dag,
    postgres_conn_id="redshift",
    sql="""
    DROP TABLE IF EXISTS public.staging_events;
    CREATE TABLE public.staging_events (
	artist varchar(256),
	auth varchar(256),
	firstname varchar(256),
	gender varchar(256),
	iteminsession int4,
	lastname varchar(256),
	length numeric(18,0),
	"level" varchar(256),
	location varchar(256),
	"method" varchar(256),
	page varchar(256),
	registration numeric(18,0),
	sessionid int4,
	song varchar(256),
	status int4,
	ts int8,
	useragent varchar(256),
	userid int4
    );
    """
)

create_table_staging_songs = PostgresOperator(
    task_id='create_table_staging_songs',
    dag=dag,
    postgres_conn_id="redshift",
    sql="""
    DROP TABLE IF EXISTS public.staging_songs;
    CREATE TABLE public.staging_songs (
	num_songs int4,
	artist_id varchar(256),
	artist_name varchar(256),
	artist_latitude numeric(18,0),
	artist_longitude numeric(18,0),
	artist_location varchar(256),
	song_id varchar(256),
	title varchar(256),
	duration numeric(18,0),
	"year" int4
    );
    """
)

create_songplays_table = PostgresOperator(
    task_id='create_songplays_table',
    dag=dag,
    postgres_conn_id="redshift",
    sql="""
    DROP TABLE IF EXISTS public.songplays;
    CREATE TABLE public.songplays (
	playid varchar(32) NOT NULL,
	start_time timestamp NOT NULL,
	userid int4 NOT NULL,
	"level" varchar(256),
	songid varchar(256),
	artistid varchar(256),
	sessionid int4,
	location varchar(256),
	user_agent varchar(256),
	CONSTRAINT songplays_pkey PRIMARY KEY (playid)
    );
    """
)

create_songs_table = PostgresOperator(
    task_id='create_songs_table',
    dag=dag,
    postgres_conn_id="redshift",
    sql="""
    DROP TABLE IF EXISTS public.songs;
    CREATE TABLE public.songs (
	songid varchar(256) NOT NULL,
	title varchar(256),
	artistid varchar(256),
	"year" int4,
	duration numeric(18,0),
	CONSTRAINT songs_pkey PRIMARY KEY (songid)
    );
    """
)

create_users_table = PostgresOperator(
    task_id='create_users_table',
    dag=dag,
    postgres_conn_id="redshift",
    sql="""
    DROP TABLE IF EXISTS public.users;
    CREATE TABLE public.users (
	userid int4 NOT NULL,
	first_name varchar(256),
	last_name varchar(256),
	gender varchar(256),
	"level" varchar(256),
	CONSTRAINT users_pkey PRIMARY KEY (userid)
    );
    """
)

create_time_table = PostgresOperator(
    task_id='create_time_table',
    dag=dag,
    postgres_conn_id="redshift",
    sql="""
    DROP TABLE IF EXISTS public."time";
    CREATE TABLE public."time" (
	start_time timestamp NOT NULL,
	"hour" int4,
	"day" int4,
	week int4,
	"month" varchar(256),
	"year" int4,
	weekday varchar(256),
	CONSTRAINT time_pkey PRIMARY KEY (start_time)
    );
    """
)

create_artists_table = PostgresOperator(
    task_id='create_artists_table',
    dag=dag,
    postgres_conn_id="redshift",
    sql="""
    DROP TABLE IF EXISTS public.artists;
    CREATE TABLE public.artists (
	artistid varchar(256) NOT NULL,
	name varchar(256),
	location varchar(256),
	lattitude numeric(18,0),
	longitude numeric(18,0)
    );
    """
)

end_operator = DummyOperator(task_id='End_execution',  dag=dag)


# Graph definition
start_operator >> [create_table_staging_events, create_table_staging_songs, create_songplays_table,
                   create_songs_table, create_time_table, create_artists_table, create_users_table]

[create_table_staging_events, create_table_staging_songs, create_songplays_table,
 create_songs_table, create_time_table, create_artists_table, create_users_table] >> end_operator