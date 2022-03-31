from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.postgres_operator import PostgresOperator
from airflow.operators import (StageToRedshiftOperator, LoadFactOperator,
                                LoadDimensionOperator, DataQualityOperator)
from helpers import SqlQueries
import configparser


# Run local
# config_file_path = './project5-data-pipelines/dwh_p.cfg'
# Run on Udacity workspace
config_file_path = './home/workspace/dwh_p.cfg'

config = configparser.ConfigParser()
config.read_file(open(config_file_path))

REDSHIFT_CONN_ID = config.get('AIRFLOW', 'REDSHIFT_CONN_ID')
AWS_CREDENTIALS_ID = config.get('AIRFLOW', 'AWS_CREDENTIALS_ID')

S3_BUCKET = config.get("S3", "S3_BUCKET")
S3_LOG_JSONPATH = config.get("S3", "S3_LOG_JSONPATH")



default_args = {
    'owner': 'udacity',
    'start_date': datetime(2022, 3, 30),
    'end_date': datetime(2022, 4, 10),
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'catchup': False,
    'email_on_retry': False
}

dag = DAG('udac_example_dag',
          default_args=default_args,
          description='Load and transform data in Redshift with Airflow',
          schedule_interval='0 * * * *'  # https://airflow.apache.org/docs/apache-airflow/1.10.1/scheduler.html
        )

start_operator =  PostgresOperator(
    task_id='Begin_execution',
    postgres_conn_id=REDSHIFT_CONN_ID,  
    dag=dag,
    sql='create_tables.sql'
)


stage_events_to_redshift = StageToRedshiftOperator(
    task_id='Stage_events',
    redshift_conn_id=REDSHIFT_CONN_ID,
    aws_credentials_id=AWS_CREDENTIALS_ID,
    table="staging_events",
    s3_bucket=S3_BUCKET,
    s3_key="log_data/2018/11/",
    json_path=S3_LOG_JSONPATH,
    file_type="json",
    dag=dag
)

stage_songs_to_redshift = StageToRedshiftOperator(
    task_id='stage_songs',
    redshift_conn_id=REDSHIFT_CONN_ID,
    aws_credentials_id=AWS_CREDENTIALS_ID,
    table="staging_songs",
    s3_bucket=S3_BUCKET,
    s3_key="song_data",
    json_path="auto",
    file_type="json",
    dag=dag
)

load_songplays_table = LoadFactOperator(
    sql_statement=SqlQueries.songplay_table_insert,
    task_id='load_songplays_table',
    redshift_conn_id=REDSHIFT_CONN_ID,
    table="songplays",
    dag=dag
)

load_user_dimension_table = LoadDimensionOperator(
    sql_statement=SqlQueries.user_table_insert,
    task_id='load_user_dim_table',
    redshift_conn_id=REDSHIFT_CONN_ID,
    table="users",
    dag=dag
)

load_song_dimension_table = LoadDimensionOperator(
    sql_statement=SqlQueries.song_table_insert,
    task_id='load_song_dim_table',
    redshift_conn_id=REDSHIFT_CONN_ID,
    table="songs",
    dag=dag
)

load_artist_dimension_table = LoadDimensionOperator(
    sql_statement=SqlQueries.artist_table_insert,
    task_id='load_artist_dim_table',
    redshift_conn_id=REDSHIFT_CONN_ID,
    table="artists",
    dag=dag
)

load_time_dimension_table = LoadDimensionOperator(
    sql_statement=SqlQueries.time_table_insert,
    task_id='load_time_dim_table',
    redshift_conn_id=REDSHIFT_CONN_ID,
    table="time",
    dag=dag
)

run_quality_checks = DataQualityOperator(
    task_id='Run_data_quality_checks',
    dag=dag,
    redshift_conn_id=REDSHIFT_CONN_ID,
    tables=['songplays', 'users', 'songs', 'artists', 'time'],
)

end_operator = DummyOperator(task_id='Stop_execution',  dag=dag)

start_operator >> stage_events_to_redshift
start_operator >> stage_songs_to_redshift

stage_events_to_redshift >> load_songplays_table
stage_songs_to_redshift >> load_songplays_table

load_songplays_table >> load_user_dimension_table
load_songplays_table >> load_song_dimension_table
load_songplays_table >> load_artist_dimension_table
load_songplays_table >> load_time_dimension_table

load_user_dimension_table >> run_quality_checks
load_song_dimension_table >> run_quality_checks
load_artist_dimension_table >> run_quality_checks
load_time_dimension_table >> run_quality_checks

run_quality_checks >> end_operator