from calendar import weekday
import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format
from pyspark.sql.functions import monotonically_increasing_id
import pandas as pd
from time import time


# Run local
config_file_path = './project4-data-lake/dl.cfg'
# Run on Udacity workspace
# config_file_path = 'dl.cfg'

config = configparser.ConfigParser()
config.read_file(open(config_file_path))

os.environ['AWS_ACCESS_KEY_ID'] = config.get('AWS','AWS_ACCESS_KEY_ID')
os.environ['AWS_SECRET_ACCESS_KEY'] = config.get('AWS','AWS_SECRET_ACCESS_KEY')
S3_BUCKET_OUTPUT = config.get("S3", "S3_BUCKET_OUTPUT")


def create_spark_session():
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark


def process_song_data(spark, input_data, output_data):
    """
        To read josn file from song_data and create songs_table and artists_table to s3 output bucket
        
        INPUT:
            spark       : from create_spark_session() function
            input_data  : s3 input bucket
            output_data : s3 output bucket
            
    """
    # get filepath to song data file
    song_data = input_data + 'song_data/*/*/*/*.json'
    
    # read song data file
    df = spark.read.json(song_data)

    # extract columns to create songs table
    songs_table = df.select("song_id","title","artist_id","year","duration").dropDuplicates()
    
    # write songs table to parquet files partitioned by year and artist
    songs_table.write.mode('overwrite').partitionBy("year", "artist_id").parquet(output_data + 'songs_table/')

    # extract columns to create artists table
    artists_table = df.select("artist_id","artist_name", col("artist_location").alias("location"), col("artist_latitude").alias("latitude"), col("artist_longitude").alias("longitude")).dropDuplicates()
    
    
    # write artists table to parquet files
    artists_table.write.mode('overwrite').parquet(output_data + 'artists_table/')


def process_log_data(spark, input_data, output_data):
    """
        To read json file from log-data and create users_table, time_table and songplays_table to s3 output bucket
        
        INPUT:
            spark       : from create_spark_session() function
            input_data  : s3 input bucket
            output_data : s3 output bucket
            
    """
    # get filepath to log data file
    log_data = input_data + 'log-data/*.json'

    # read log data file
    df = spark.read.json(log_data)
    
    # filter by actions for song plays
    df = df.filter(df.page == 'NextSong')

    # extract columns for users table    
    users_table = df.select(col("userId").alias("user_id"), col("firstName").alias("first_name"), col("lastName").alias("last_name"), "gender", "level").dropDuplicates()
    
    # write users table to parquet files
    users_table.write.mode('overwrite').parquet(output_data + 'users_table/')
    
    # create datetime column from original timestamp column
    get_datetime = udf(lambda x: datetime.fromtimestamp(x / 1000.0))  # refer: Lesson 2 - from Data Wrangling with Spark Exercise '4_data_wrangling' - Calculating Statistics by Hour
    df = df.withColumn("start_time", get_datetime(df.ts))
    
    # extract columns to create time table
    time_table = df.withColumn("hour",hour("start_time"))\
                    .withColumn("day",dayofmonth("start_time"))\
                    .withColumn("week",weekofyear("start_time"))\
                    .withColumn("month",month("start_time"))\
                    .withColumn("year",year("start_time"))\
                    .withColumn("weekday",weekday("start_time"))\
                    .select("start_time","hour", "day", "week", "month", "year", "weekday").dropDuplicates()   # refer: https://knowledge.udacity.com/questions/806719
    
    # write time table to parquet files partitioned by year and month
    time_table.write.mode('overwrite').partitionBy("year", "month").parquet(output_data + 'time_table/')
    
    # read in song data to use for songplays table
    song_df = spark.read.parquet(output_data + 'songs_table/')

    # extract columns from joined song and log datasets to create songplays table 
    songplays_table = df.join(song_df, song_df.title == df.song, how='inner').withColumn("songplay_id", monotonically_increasing_id()) \
        .select(df.start_time, \
            col("userId").alias("user_id"), \
                "level", \
                "song_id", \
                "artist_id", \
                col("sessionId").alias("session_id"), \
                "location", \
                col("userAgent").alias("user_agent") \
            )       # refer https://knowledge.udacity.com/questions/270371

    # write songplays table to parquet files partitioned by year and month
    songplays_table.drop_duplicates().write.mode('overwrite').partitionBy("year", "month").parquet(output_data+'songplays_table/')


def main():
    spark = create_spark_session()
    input_data = "s3a://udacity-dend/"
    output_data = "s3a://{}/".format(S3_BUCKET_OUTPUT)
    
    t0 = time()
    process_song_data(spark, input_data, output_data)    
    process_log_data(spark, input_data, output_data)

    print('etl done in: {0:.2f} sec(s)\n'.format(time()-t0))


if __name__ == "__main__":
    main()
