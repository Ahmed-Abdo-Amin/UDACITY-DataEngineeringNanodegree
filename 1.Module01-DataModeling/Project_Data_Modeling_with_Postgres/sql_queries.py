# Create Fact Table: 1. songplays
songplay_table_create = ("""
CREATE TABLE IF NOT EXISTS songplays(
songplay_id varchar PRIMARY KEY, 
start_time TIMESTAMP, 
user_id int not null, 
level text, 
song_id varchar, 
artist_id varchar, 
session_id int, 
location text, 
user_agent varchar
)
""")
# create Dimension Tables
# 2. users
user_table_create = ("""
CREATE TABLE IF NOT EXISTS users(
user_id int primary key, 
first_name text not null, 
last_name text not null, 
gender char, 
level text
)
""")
# 3. songs
song_table_create = ("""
CREATE TABLE IF NOT EXISTS songs(
song_id varchar primary key, 
title text not null, 
artist_id varchar, 
year int, 
duration real
)
""")
# 4. artists 
artist_table_create = ("""
CREATE TABLE IF NOT EXISTS artists(
artist_id varchar primary key, 
artist_name text not null, 
artist_location text, 
artist_latitude real, 
artist_longitude real
)
""")
# 5. time
time_table_create = ("""
CREATE TABLE IF NOT EXISTS time(
start_time TIMESTAMP PRIMARY KEY, 
hour int, 
day int, 
week int, 
month int, 
year int, 
weekday int
)
""")

# Drop All tables of Sparkify database
songplay_table_drop = "DROP TABLE IF EXISTS songplays"
user_table_drop = "DROP TABLE IF EXISTS users"
song_table_drop = "DROP TABLE IF EXISTS songs"
artist_table_drop = "DROP TABLE IF EXISTS artists"
time_table_drop = "DROP TABLE IF EXISTS time"

# INSERT RECORDS

songplay_table_insert = ("""
INSERT INTO songplays (songplay_id, start_time, user_id, level, song_id, artist_id, session_id, location, user_agent)
VALUES 
(
%s, %s, %s, %s, %s, %s, %s, %s, %s
)ON CONFLICT DO NOTHING
""")

user_table_insert = ("""
INSERT INTO users (user_id, first_name, last_name, gender, level) 
VALUES 
(
%s, %s, %s, %s, %s
)
ON CONFLICT (user_id)
DO UPDATE 
SET level = EXCLUDED.level
""")

song_table_insert = ("""
INSERT INTO songs (song_id, title, artist_id, year, duration) 
VALUES 
(
%s, %s, %s, %s, %s
)
ON CONFLICT (song_id)
DO UPDATE 
SET year = EXCLUDED.year
""")

artist_table_insert = ("""
INSERT INTO artists (artist_id, artist_name, artist_location, artist_latitude,artist_longitude) 
VALUES 
(
%s, %s, %s, %s, %s
)
ON CONFLICT (artist_id)
DO NOTHING
""")

time_table_insert = ("""
INSERT INTO time (start_time, hour, day, week, month, year, weekday) 
VALUES 
(
%s, %s, %s, %s, %s, %s, %s
)ON CONFLICT DO NOTHING
""")

# FIND SONGS

song_select = ("""
SELECT s.song_id, a.artist_id 
FROM songs s
JOIN artists a
ON (s.artist_id = a.artist_id) 
WHERE (s.title = %s AND a.artist_name = %s AND s.duration = %s)
""")

# Query lists
create_table_queries = [songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]