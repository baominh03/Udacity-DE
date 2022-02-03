# HANDLE CSV FILE

## To run on Udacity workspace
# csv_file = 'event_datafile_new.csv'
# event_folder = 'event_data'

## To run on locally
csv_file = './project2-data-modeling-with-cassandra/event_datafile_new.csv'
event_folder = './project2-data-modeling-with-cassandra/event_data'

# DROP TABLES

# add IF EXIST following as Project Rubric
song_table_drop = "DROP TABLE IF EXISTS song_table"
artist_table_drop = "DROP TABLE IF EXISTS artist_table"
user_table_drop = "DROP TABLE IF EXISTS user_table"


# CREATE TABLES

song_table_create = ("""
CREATE TABLE IF NOT EXISTS song_table (
    session_id int, 
    item_in_session int, 
    artist text, 
    song_title text, 
    length float, 
    PRIMARY KEY (session_id, item_in_session)
);
""")

artist_table_create = ("""
CREATE TABLE IF NOT EXISTS artist_table (
    user_id int, 
    session_id int, 
    artist text, 
    song_title text, 
    item_in_session int,
    first_name text, 
    last_name text,
    PRIMARY KEY (user_id, session_id, item_in_session)
);
""")

user_table_create = ("""
CREATE TABLE IF NOT EXISTS user_table (
    song_title text, 
    user_id int, 
    first_name text, 
    last_name text,
    PRIMARY KEY (song_title, user_id)
);
""")

# INSERT RECORDS

song_table_insert = ("""
INSERT INTO song_table (session_id, item_in_session, artist, song_title, length)
VALUES (%s, %s, %s, %s, %s)
""")

artist_table_insert = ("""
INSERT INTO artist_table (user_id, session_id, artist, song_title, item_in_session, first_name, last_name)
VALUES (%s, %s, %s, %s, %s, %s, %s)
""")

user_table_insert = ("""
INSERT INTO user_table (song_title, user_id, first_name, last_name)
VALUES (%s, %s, %s, %s)
""")

# QUERY

query1 = ("""SELECT artist, song_title, length
FROM song_table WHERE session_id=338 AND item_in_session = 4
""")

query2 = ("""SELECT artist, song_title, item_in_session, first_name, last_name
FROM artist_table WHERE user_id=10 AND session_id=182 
""")

query3 = ("""SELECT first_name, last_name
FROM user_table WHERE song_title='All Hands Against His Own' 
""")

# QUERY LISTS

create_table_queries = [user_table_create,
                        song_table_create, artist_table_create]
drop_table_queries = [user_table_drop,
                      song_table_drop, artist_table_drop]
