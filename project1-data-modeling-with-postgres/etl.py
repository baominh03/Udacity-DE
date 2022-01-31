import os
import glob
import psycopg2
import pandas as pd
from sql_queries import *


def process_song_file(cur, filepath):
    """
    This procedure processes a song file whose filepath has been provided as an argument.
    It extracts the song information in order to store it into the songs table.
    Then it extracts the artist information in order to store it into the artists table.

    INPUTS: 
    * cur the cursor variable
    * filepath the file path to the song file
    """
    # open song file
    df = pd.read_json(filepath, lines=True)

    # insert song record
    song_data = df[['song_id', 'title', 'artist_id', 'year', 'duration']].values[0].tolist()
    cur.execute(song_table_insert, song_data)
    cur.execute(song_table_insert, ('TBD','TBD','TBD',0,0)) # add initial song_id if song_id is Null from log dataset
    
    # insert artist record
    artist_data = df[['artist_id', 'artist_name', 'artist_location', 'artist_latitude', 'artist_longitude']].values[0].tolist()
    cur.execute(artist_table_insert, artist_data)
    cur.execute(artist_table_insert, ('TBD','TBD','TBD', None, None)) # add initial artist_id if artist_id is Null from log dataset


def process_log_file(cur, filepath):
    """
    This procedure processes a log file whose filepath has been provided as an argument.
    It filters the log information with page = NextSong  
    It converts timestamp to datetime the log information
    Then It extracts the time information in order to store it into the time table.
    Then it extracts the user information in order to store it into the user table.
    Use song information, artist name, length from the log information to select songid and artistid from songs table and artists table (refer process_song_file function)
    Then insert handled log infomation with songid and artistid into the songplay table

    INPUTS: 
    * cur the cursor variable
    * filepath the file path to the log file
    """
    # open log file
    df = pd.read_json(filepath , lines=True)

    # filter by NextSong action
    df  = df [df.page.eq("NextSong")]

    # convert timestamp column to datetime
    t = pd.to_datetime(df['ts'], unit='ms')  #refer https://pandas.pydata.org/docs/reference/api/pandas.to_datetime.html
    
    # extract the time information
    time_data = (t, t.dt.hour, t.dt.day, t.dt.week, t.dt.month, t.dt.year, t.dt.weekday)  #refer https://knowledge.udacity.com/questions/39340
    column_labels = ("start_time", "hour", "day", "week", "month", "year", "weekday")
    time_df = pd.DataFrame(dict(zip(column_labels, time_data)))  #refer https://knowledge.udacity.com/questions/39340

    # insert time data records
    for i, row in time_df.iterrows():
        cur.execute(time_table_insert, list(row))

    # extract user profiles and remove any duplicate values, keeping the first one
    user_df = df[['userId', 'firstName', 'lastName', 'gender', 'level']].drop_duplicates(subset = 'userId', keep = 'first') #Refer: https://knowledge.udacity.com/questions/684174

    # insert user data records
    for i, row in user_df.iterrows():
        cur.execute(user_table_insert, row)

    # insert songplay records
    for index, row in df.iterrows():
        
        # get songid and artistid from song and artist tables by song, artist, length from log infomation
        cur.execute(song_select, (row.song, row.artist, row.length))
        results = cur.fetchone()
        
        if results:
            songid, artistid = results
        else:
            songid, artistid = 'TBD', 'TBD'

        # insert songplay record
        songplay_data = ([pd.to_datetime(row.ts, unit='ms'), row.userId, row.level, songid, artistid, row.sessionId, row.location, row.userAgent])  #refer https://knowledge.udacity.com/questions/648754
        cur.execute(songplay_table_insert, songplay_data)


def process_data(cur, conn, filepath, func):
    """
    This procedure processes data whose filepath and function have been provided as an argument.
    It handles filepath in order to read all data file
    Then execute then function name provied from variable
    
    INPUTS: 
    * cur the cursor variable
    * conn the connection variable
    * filepath the file path to the log file
    * func function name
    """
    # get all files matching extension from directory
    all_files = []
    for root, dirs, files in os.walk(filepath):
        files = glob.glob(os.path.join(root,'*.json'))
        for f in files :
            all_files.append(os.path.abspath(f))

    # get total number of files found
    num_files = len(all_files)
    print('{} files found in {}'.format(num_files, filepath))

    # iterate over files and process
    for i, datafile in enumerate(all_files, 1):
        func(cur, datafile)
        conn.commit()
        print('{}/{} files processed.'.format(i, num_files))


def main():
    conn = psycopg2.connect("host=127.0.0.1 dbname=sparkifydb user=student password=student")
    cur = conn.cursor()

    process_data(cur, conn, filepath='data/song_data', func=process_song_file)
    process_data(cur, conn, filepath='data/log_data', func=process_log_file)

    conn.close()


if __name__ == "__main__":
    main()