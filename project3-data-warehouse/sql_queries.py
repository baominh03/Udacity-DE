import configparser

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

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs"
songplay_table_drop = "DROP TABLE IF EXISTS songplay_table"
user_table_drop = "DROP TABLE IF EXISTS user_table"
song_table_drop = "DROP TABLE IF EXISTS song_table"
artist_table_drop = "DROP TABLE IF EXISTS artist_table"
time_table_drop = "DROP TABLE IF EXISTS time_table"

# CREATE TABLES

staging_events_table_create = ("""
    CREATE TABLE staging_events (
        artist VARCHAR,
        auth VARCHAR,
        firstName VARCHAR,
        gender CHAR(1),
        itemInSession INT,
        lastName  VARCHAR,
        length FLOAT, 
        level VARCHAR(4),
        location VARCHAR,
        method VARCHAR,
        page VARCHAR,
        registration FLOAT, 
        sessionId INT,
        song VARCHAR,
        status INT,
        ts BIGINT,
        userAgent VARCHAR,
        userId INT
    );
""")

staging_songs_table_create = ("""
    CREATE TABLE staging_songs (
        num_songs INT,
        artist_id VARCHAR(18),
        artist_latitude FLOAT,
        artist_longitude FLOAT,
        artist_location VARCHAR,
        artist_name VARCHAR,
        song_id VARCHAR(18),
        title VARCHAR,
        duration FLOAT,
        year INT
     );
""")

# The SERIAL command in Postgres is not supported in Redshift. The equivalent in redshift is IDENTITY(0,1)
songplay_table_create = ("""
    CREATE TABLE songplay_table (
        songplay_id INTEGER  IDENTITY(0,1),
        start_time TIMESTAMP NOT NULL sortkey distkey,
        user_id INT NOT NULL,
        level VARCHAR(4) NOT NULL,
        song_id VARCHAR(18) NOT NULL,
        artist_id VARCHAR(18) NOT NULL,
        session_id INT NOT NULL,
        user_agent VARCHAR NOT NULL,
        location VARCHAR NOT NULL
    );
""")

user_table_create = ("""
    CREATE TABLE user_table (
        user_id INT NOT NULL sortkey,
        first_name VARCHAR,
        last_name VARCHAR,
        gender CHAR(1),
        level VARCHAR(4)
    );
""")

song_table_create = (""" 
    CREATE TABLE song_table (
        song_id VARCHAR(18) NOT NULL sortkey,
        song_title VARCHAR NOT NULL,
        artist_id VARCHAR(18) NOT NULL,
        year INTEGER NOT NULL,
        duration FLOAT
    );
""")

artist_table_create = ("""
    CREATE TABLE artist_table (
        artist_id VARCHAR(18) NOT NULL sortkey,
        artist_name VARCHAR NOT NULL,
        artist_location VARCHAR NOT NULL,
        artist_latitude FLOAT NOT NULL,
        artist_longitude FLOAT NOT NULL
    );
""")

time_table_create = ("""
    CREATE TABLE time_table (
        start_time timestamp NOT NULL distkey sortkey,
        hour INT NOT NULL,
        day INT NOT NULL,
        week INT NOT NULL, 
        month INT NOT NULL,
        year INT NOT NULL,
        weekday INT NOT NULL        
    );
""")

# STAGING TABLES

# refer: https://knowledge.udacity.com/questions/514188
# to resolve issue: psycopg2.errors.InternalError_: Load into table 'staging_songs' failed.  Check 'stl_load_errors' system table for details.
staging_events_copy = ("""
SET search_path TO sparkifyDWH;
COPY staging_events FROM {}
CREDENTIALS 'aws_iam_role={}'
COMPUPDATE OFF region 'us-west-2'
TIMEFORMAT as 'epochmillisecs'
TRUNCATECOLUMNS BLANKSASNULL EMPTYASNULL
FORMAT AS JSON {}
""").format(LOG_DATA, DWH_ROLE_ARN, LOG_JSONPATH)  # Refer COPY from JSON format: https://docs.aws.amazon.com/redshift/latest/dg/copy-usage_notes-copy-from-json.html

staging_songs_copy = ("""
COPY staging_events FROM {}
CREDENTIALS 'aws_iam_role={}'
COMPUPDATE OFF region 'us-west-2'
TIMEFORMAT as 'epochmillisecs'
TRUNCATECOLUMNS BLANKSASNULL EMPTYASNULL
FORMAT AS JSON 'auto'
""").format(SONG_DATA, DWH_ROLE_ARN)  # Refer COPY from 'auto' format: https://docs.aws.amazon.com/redshift/latest/dg/copy-usage_notes-copy-from-json.html

# FINAL TABLES
songplay_table_insert = ("""
    INSERT INTO songplay_table (start_time, user_id, level, song_id, artist_id,  session_id, user_agent, location )
                       
    SELECT (timestamp 'epoch' + ts/1000 * interval '1 second') AS start_time,
        userId user_id, level, song_id, artist_id, sessionId session_id, userAgent user_agent, location
    FROM staging_songs ss 
    JOIN staging_events se ON (ss.artist_name = se.artist
                               AND ss.title = se.song)
    WHERE page = 'NextSong'
""")  # Refer how to convert timestamp to datetime in redshift: https://knowledge.udacity.com/questions/154533

user_table_insert = ("""
    INSERT INTO user_table (user_id, first_name, last_name, gender, level)

    SELECT userId as user_id, firstName as first_name, lastName as last_name, gender, level 
    FROM staging_events
    ON CONFLICT (user_id)
    DO UPDATE SET level = excluded.level
""")

song_table_insert = ("""
    INSERT INTO song_table (song_id, song_title, artist_id, year, duration)

    SELECT song_id, title as song_title, artist_id, year, duration
    FROM staging_songs
    ON CONFLICT (song_id) DO NOTHING
""")

artist_table_insert = ("""
    INSERT INTO artist_table (artist_id, name, artist_location, artist_latitude, artist_longitude)

    SELECT artist_id, artist_name, location as artist_location, latitude as artist_latitude, longitude as artist_longitude
    FROM staging_songs
    ON CONFLICT (artist_id) DO NOTHING
""")

time_table_insert = ("""
    INSERT INTO time_table (start_time, hour, day, week, month, year, weekday)

    SELECT start_time,
    EXTRACT(hour  from s.start_time) as hour,
    EXTRACT(day   from s.start_time) as day,
    EXTRACT(week  from s.start_time) as week,
    EXTRACT(month from s.start_time) as month,
    EXTRACT(year  from s.start_time) as year,
    EXTRACT(dow   from s.start_time) as weekday
    FROM songplay_table
    ON CONFLICT (start_time) DO NOTHING
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create,
                        user_table_drop, song_table_drop, artist_table_drop, time_table_drop, songplay_table_drop]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop,
                      user_table_drop, song_table_drop, artist_table_drop, time_table_drop, songplay_table_drop]
copy_table_queries = [staging_songs_copy, staging_events_copy]
insert_table_queries = [user_table_insert, time_table_insert,
                        song_table_insert, artist_table_insert, songplay_table_insert]
