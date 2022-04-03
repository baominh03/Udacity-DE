from datetime import datetime, timedelta
import os
from airflow import DAG
from pyspark.sql import SparkSession
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.postgres_operator import PostgresOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators import (StageToRedshiftOperator, LoadFactOperator,
                                LoadDimensionOperator, DataQualityOperator)
from helpers import SqlQueries
import configparser
import logging


config_file_path = './home/workspace/dwh_p.cfg'

config = configparser.ConfigParser()
config.read_file(open(config_file_path))

REDSHIFT_CONN_ID = config.get('AIRFLOW', 'REDSHIFT_CONN_ID')
AWS_CREDENTIALS_ID = config.get('AIRFLOW', 'AWS_CREDENTIALS_ID')

S3_BUCKET = config.get("S3", "S3_BUCKET")

os.environ['AWS_ACCESS_KEY_ID'] = config.get('AWS', 'KEY')
os.environ['AWS_SECRET_ACCESS_KEY'] = config.get(
    'AWS', 'SECRET')
S3_BUCKET_OUTPUT = config.get("S3", "S3_BUCKET_OUTPUT")

default_args = {
    'owner': 'udacity',
    'start_date': datetime(2022, 4, 3),
    'end_date': datetime(2022, 4, 4),
    'retries': 3,
    'retry_delay': timedelta(seconds=10),
    'catchup': False,
    'email_on_retry': False
}


def process_upload(spark_df, output_folder):
    output_data = "s3a://{}/".format(S3_BUCKET_OUTPUT) + output_folder + '/'

    logging.info('upload staging s3 = [{}]'.format(output_data))
    spark_df.write.mode('overwrite').parquet(output_data)


def upload_to_staging():

    spark = SparkSession.builder\
        .config("spark.jars.repositories", "https://repos.spark-packages.org/")\
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0,saurfang:spark-sas7bdat:2.0.0-s_2.11")\
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")\
        .config("spark.hadoop.fs.s3a.access.key", os.environ['AWS_ACCESS_KEY_ID'])\
        .config("spark.hadoop.fs.s3a.secret.key", os.environ['AWS_SECRET_ACCESS_KEY'])\
        .enableHiveSupport().getOrCreate()
    df_imm_temp = spark.read.parquet(
        'home/workspace/output_data_workspace/df_imm')
    df_airport_temp = spark.read.parquet(
        'home/workspace/output_data_workspace/df_airport')
    df_city_temp = spark.read.parquet(
        'home/workspace/output_data_workspace/df_city')

    process_upload(df_imm_temp, 'staging_immg')
    process_upload(df_airport_temp, 'staging_airport')
    process_upload(df_city_temp, 'staging_city')


dag = DAG('etl_dags',
          default_args=default_args,
          description='Capstone project 6',
          schedule_interval='0 * * * *'
          )

start_operator = PostgresOperator(
    task_id='Begin_execution',
    postgres_conn_id=REDSHIFT_CONN_ID,
    dag=dag,
    sql='create_tables.sql'
)

# start_operator = DummyOperator(task_id='Begin_execution',  dag=dag)

stage_to_s3 = PythonOperator(
    task_id='stage_to_s3_task',
    python_callable=upload_to_staging,
    dag=dag
)

stage_immg_to_redshift = StageToRedshiftOperator(
    task_id='stage_immg_to_redshift',
    redshift_conn_id=REDSHIFT_CONN_ID,
    aws_credentials_id=AWS_CREDENTIALS_ID,
    table="staging_immg",
    s3_bucket=S3_BUCKET,
    s3_key="staging_immg",
    dag=dag
)

stage_airport_to_redshift = StageToRedshiftOperator(
    task_id='stage_airport_to_redshift',
    redshift_conn_id=REDSHIFT_CONN_ID,
    aws_credentials_id=AWS_CREDENTIALS_ID,
    table="staging_airport",
    s3_bucket=S3_BUCKET,
    s3_key="staging_airport",
    dag=dag
)

stage_city_to_redshift = StageToRedshiftOperator(
    task_id='stage_city_to_redshift',
    redshift_conn_id=REDSHIFT_CONN_ID,
    aws_credentials_id=AWS_CREDENTIALS_ID,
    table="staging_city",
    s3_bucket=S3_BUCKET,
    s3_key="staging_city",
    dag=dag
)

load_immggration_table = LoadFactOperator(
    sql_statement=SqlQueries.immg_table_insert,
    task_id='load_immggration_table',
    redshift_conn_id=REDSHIFT_CONN_ID,
    table="us_immgration",
    dag=dag
)

load_airport_table = LoadDimensionOperator(
    sql_statement=SqlQueries.air_table_insert,
    task_id='load_airport_table',
    redshift_conn_id=REDSHIFT_CONN_ID,
    table="airport",
    dag=dag
)

load_city_table = LoadDimensionOperator(
    sql_statement=SqlQueries.city_table_insert,
    task_id='load_city_table',
    redshift_conn_id=REDSHIFT_CONN_ID,
    table="city_demographic",
    dag=dag
)

run_quality_checks = DataQualityOperator(
    task_id='Run_data_quality_checks',
    dag=dag,
    redshift_conn_id=REDSHIFT_CONN_ID,
    tables=['city_demographic', 'airport', 'us_immgration'],
)

end_operator = DummyOperator(task_id='Stop_execution',  dag=dag)

start_operator >> stage_to_s3

stage_to_s3 >> stage_immg_to_redshift
stage_to_s3 >> stage_airport_to_redshift
stage_to_s3 >> stage_city_to_redshift

stage_immg_to_redshift >> load_immggration_table
stage_airport_to_redshift >> load_airport_table
stage_city_to_redshift >> load_city_table

load_immggration_table >> run_quality_checks
load_airport_table >> run_quality_checks
load_city_table >> run_quality_checks

run_quality_checks >> end_operator
