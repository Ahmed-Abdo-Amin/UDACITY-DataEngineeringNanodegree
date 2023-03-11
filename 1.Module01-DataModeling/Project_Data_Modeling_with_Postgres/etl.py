# Import required labraries
import os
import glob
import psycopg2
import pandas as pd
import numpy as np
from sql_queries import *


###############----------processLog_file----------##########################
"""
    This procedure processes a song file whose filepath has been provided as an arugment.
    It extracts the song information in order to store it into the songs table.
    Then it extracts the artist information in order to store it into the artists table.
    INPUTS:
    1. cur ------> the cursor variable
    2. filePath -> the file path to the song file
"""

def processSong_file(cur, filePath):
     # open song file
    df_songFile = pd.read_json(filePath, lines=True)

    # insert song record
    songData = list(df_songFile[['song_id', 'title', 'artist_id', 'year', 'duration']].values[0])
    cur.execute(song_table_insert, songData)
    
    # insert artist record
    artistData = list(df_songFile[['artist_id', 'artist_name', 'artist_location', 'artist_latitude', 'artist_longitude']].values[0])
    cur.execute(artist_table_insert, artistData)
    

###############----------processLog_file----------##########################
"""
    This procedure processes a log file whose filepath has been provided as an arugment.
    It extracts the song start time information, tansforms it and then store it into the time table.
    Then it extracts the users information in order to store it into the users table.
    Finally it extrats informations from songs table, artists table and original log file to store it into the songplays table.
    INPUTS:
     1.cur ------> the cursor variable
     2.filePath -> the file path to the song file
"""

def processLog_file(cur, filePath):
     # open log file
    df_logFile = pd.read_json(filePath, lines=True)

    # filter by NextSong action
    df_logFile = df_logFile[df_logFile['page']=='NextSong']

    # convert timestamp column to datetime
    t = pd.to_datetime(df_logFile['ts'], unit='ms')
    
    # insert time data records
    time_data = (df_logFile['ts'].values.tolist(), t.dt.hour.values.tolist(), 
             t.dt.day.values.tolist(), t.dt.week.values.tolist(), 
             t.dt.month.values.tolist(), t.dt.year.values.tolist(), 
             t.dt.weekday.values.tolist(), )
    column_labels = ('start_time', 'hour', 'day', 'week', 'month', 'year', 'weekday')
    time_df = pd.DataFrame(dict(zip(column_labels, time_data)))

    for i, row in time_df.iterrows():
        cur.execute(time_table_insert, list(row))

    # load user table
    user_df = df_logFile[['userId', 'firstName', 'lastName', 'gender', 'level']]

    # insert user records
    for i, row in user_df.iterrows():
        cur.execute(user_table_insert, row)

    # insert songplay records
    for index, row in df_logFile.iterrows():
        
        # get songid and artistid from song and artist tables
        cur.execute(song_select, (row.song, row.artist, row.length))
        results = cur.fetchone()
        
        if results:
            songid, artistid = results
        else:
            songid, artistid = None, None

        # insert songplay record
        songplay_data = (index, row.ts, row.userId, row.level, songid, artistid, row.sessionId, row.location, row.userAgent)
        cur.execute(songplay_table_insert, songplay_data)
    
###############----------processData----------##########################
"""Processing one file at a time and losding data into corresponding tables
   This procedure processes a song or log file whose filepath has been provided as an arugment at a time by calling corresponding functions.
   INPUTS:
    1. conn -----> the database connection variable
    2. cur ------> the cursor variable
    3. filePath -> the file path to the song file
    4. func -----> the func variable which can be either processSong_file or processLog_file
"""
def processData(conn, cur, filePath, func):
    # Get all files matching extension (*.json) from directory
    all_files = []
    for root, dirs, files in os.walk(filepath):
        files = glob.glob(os.path.join(root,'*.json'))
        for f in files :
            all_files.append(os.path.abspath(f))
            
    # Get total number of files found
    num_files = len(all_files)
    print('{} files found in {}'.format(num_files, filepath))

    # Iterate over files and process
    for i, datafile in enumerate(all_files, 1):
        func(cur, datafile)
        conn.commit()
        print('{}/{} files processed.'.format(i, num_files))


def main():
    # connect to sparkifydb database (UDACITY-Data-Engineer)
    try:
        conn = psycopg2.connect(database='sparkifydb', user='student', password='student')
    except psycopg2.Error as e:
        print("Error: Could not make connection to the postgres sparkifydb database")
        print(e)
    try:
        cur = conn.cursor()
    except:
        print("Error: Could not make curser to the sparkifydb database")
        print(e)
    conn.set_session(autocommit=True)
    
    processData(conn, cur, filePath='data/song_data', func=processSong_file)
    processData(conn, cur, filePath='data/log_data', func=processLog_file)

    cur.close()
    conn.close()


if __name__ == "__main__":
    main()