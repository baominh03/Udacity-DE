from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.contrib.hooks.aws_hook import AwsHook
import configparser
import os
from pyspark.sql import SparkSession
import logging
from time import time

class LoadToStageOperator(BaseOperator):
    ui_color = '#358140'


    @apply_defaults
    def __init__(self,
                 output_folder,
                 s3_output_folder,
                 *args, **kwargs):

        super(LoadToStageOperator, self).__init__(*args, **kwargs)
        self.output_folder = output_folder
        self.s3_output_folder = s3_output_folder

    def process_upload(spark_df, output_folder):
        t0 = time()
        output_data = "s3a://{}/".format(S3_BUCKET_OUTPUT) + output_folder + '/'
    
        logging.info('upload staging s3 = [{}]'.format(output_data))
        spark_df.write.mode('overwrite').parquet(output_data)
        logging.info('upload DONE staging s3 = [{}] in: {0:.2f} sec(s)\n'.format(output_data, time()-t0))


    def execute(self, context):
        spark = SparkSession \
            .builder \
            .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
            .getOrCreate()
        logging.info("spark connection created.")
        
        df = spark.read.parquet('output_data_workspace/' + self.output_folder)
        logging.info("Read parquet [{}] DONE")
        logging.info("=== Preparing data from S3 to Redshift ===")
        self.process_upload(df, self.s3_output_folder)
     





