import configparser
import sql_queries_dist

# Run local
config_file_path = './project3-data-warehouse/dwh.cfg'
# Run on Udacity workspace
# config_file_path = 'dwh.cfg'
# CONFIG

config = configparser.ConfigParser()
config.read(config_file_path)

LOG_DATA = config.get('S3', 'LOG_DATA')
LOG_JSONPATH = config.get('S3', 'LOG_JSONPATH')
SONG_DATA = config.get('S3', 'SONG_DATA')

# autofill after running create_cluster.py
DWH_ROLE_ARN = config.get("IAM_ROLE", "DWH_ROLE_ARN")

# DROP TABLES

#add IF EXIST following as Project Rubric
staging_events_table_drop = sql_queries_dist.staging_events_table_drop
staging_songs_table_drop = sql_queries_dist.staging_songs_table_drop
songplay_table_drop = sql_queries_dist.songplay_table_drop
user_table_drop = sql_queries_dist.user_table_drop
song_table_drop = sql_queries_dist.song_table_drop
artist_table_drop = sql_queries_dist.artist_table_drop
time_table_drop = sql_queries_dist.time_table_drop

# CREATE TABLES

staging_events_table_create = sql_queries_dist.staging_events_table_create

staging_songs_table_create = sql_queries_dist.staging_songs_table_create

songplay_table_create = ("""
CREATE TABLE IF NOT EXISTS songplay (
    songplay_id INTEGER  IDENTITY(0,1),
    start_time TIMESTAMP NOT NULL,
    user_id INT NOT NULL,
    level VARCHAR(4) NOT NULL,
    song_id VARCHAR(18) NOT NULL,
    artist_id VARCHAR(18) NOT NULL,
    session_id INT NOT NULL,
    location VARCHAR NOT NULL,
    user_agent VARCHAR NOT NULL
);
""")

user_table_create = ("""
CREATE TABLE IF NOT EXISTS users (
    user_id INT NOT NULL,
    first_name VARCHAR,
    last_name VARCHAR,
    gender CHAR(1),
    level VARCHAR(4)
);
""")

song_table_create = ("""
CREATE TABLE IF NOT EXISTS songs (
    song_id VARCHAR(18) NOT NULL,
    song_title VARCHAR NOT NULL,
    artist_id VARCHAR(18) NOT NULL,
    year INT NOT NULL,
    duration FLOAT NOT NULL
);
""")

artist_table_create = ("""
CREATE TABLE IF NOT EXISTS artists (
    artist_id VARCHAR(18) NOT NULL,
    artist_name VARCHAR NOT NULL,
    artist_location VARCHAR,
    artist_latitude FLOAT,
    artist_longitude FLOAT
);
""")

time_table_create = ("""
CREATE TABLE IF NOT EXISTS time (
    start_time TIMESTAMP NOT NULL,
    hour INT NOT NULL,
    day INT NOT NULL,
    week INT NOT NULL,
    weekday INT NOT NULL,
    month INT NOT NULL,
    year INT NOT NULL
);
""")

# STAGING TABLES

staging_events_copy = sql_queries_dist.staging_events_copy 

staging_songs_copy = sql_queries_dist.staging_songs_copy


# INSERT RECORDS
# FINAL TABLES

songplay_table_insert = sql_queries_dist.songplay_table_insert

user_table_insert = sql_queries_dist.user_table_insert

song_table_insert = sql_queries_dist.song_table_insert

artist_table_insert = sql_queries_dist.artist_table_insert

time_table_insert = sql_queries_dist.time_table_insert

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, time_table_create, song_table_create, artist_table_create, user_table_create, songplay_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
