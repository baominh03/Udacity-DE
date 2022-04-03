from calendar import weekday
import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col, monotonically_increasing_id, year, month, dayofmonth, hour, weekofyear, date_format, dayofweek
from pyspark.sql.types import TimestampType
import pandas as pd
from time import time


config_file_path = 'dwh_p.cfg'

config = configparser.ConfigParser()
config.read_file(open(config_file_path))

config = configparser.ConfigParser()
config.read_file(open(config_file_path))

os.environ['AWS_ACCESS_KEY_ID'] = config.get('AWS', 'KEY')
os.environ['AWS_SECRET_ACCESS_KEY'] = config.get(
    'AWS', 'SECRET')
S3_BUCKET_OUTPUT = config.get("S3", "S3_BUCKET_OUTPUT")



def process_upload(spark_df, output_folder):
    output_data = "s3a://{}/".format(S3_BUCKET_OUTPUT) + output_folder + '/'
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    print('upload staging s3 = [{}]'.format(output_data))
    spark_df.write.mode('overwrite').parquet(output_data)
    print('upload DONE staging s3 = [{}]'.format(output_data))


