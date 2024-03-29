# DROP TABLES

songplay_table_drop = "DROP TABLE IF EXISTS songplays"
user_table_drop = "DROP TABLE IF EXISTS users"
song_table_drop = "DROP TABLE IF EXISTS songs"
artist_table_drop = "DROP TABLE IF EXISTS artists"
time_table_drop = "DROP TABLE IF EXISTS time"

# CREATE TABLES

songplay_table_create = ("""CREATE TABLE IF NOT EXISTS songplays(songplay_id SERIAL PRIMARY KEY,\
                                                                 start_time bigint,\
                                                                 user_id int NOT NULL,\
                                                                 level text,\
                                                                 song_id varchar,\
                                                                 artist_id varchar,\
                                                                 session_id int NOT NULL, \
                                                                 location varchar, \
                                                                 user_agent varchar)""")

user_table_create = ("""CREATE TABLE IF NOT EXISTS users(user_id int PRIMARY KEY NOT NULL,\
                                                         first_name varchar ,\
                                                         last_name varchar ,\
                                                         gender text,\
                                                         level text )""")

song_table_create = ("""CREATE TABLE IF NOT EXISTS songs(song_id varchar PRIMARY KEY NOT NULL,\
                                                         title varchar,\
                                                         artist_id varchar NOT NULL,\
                                                         year int,\
                                                         duration float(8) )""")

artist_table_create = ("""CREATE TABLE IF NOT EXISTS artists(artist_id varchar PRIMARY KEY NOT NULL,\
                                                             name varchar,\
                                                             location varchar,\
                                                             latitude varchar,\
                                                             longitude varchar)""")

time_table_create = ("""CREATE TABLE IF NOT EXISTS time(start_time time NOT NULL,\
                                                        hour int,\
                                                        day int NOT NULL,\
                                                        week int NOT NULL,\
                                                        month int,\
                                                        year int NOT NULL,\
                                                        weekday int,\
                                                        PRIMARY KEY(start_time, day, month, year))""")

# INSERT RECORDS

songplay_table_insert = ("""INSERT INTO songplays(start_time,user_id,level,song_id,artist_id,session_id,location,user_agent) VALUES (%s,%s,%s,%s,%s,%s,%s,%s)""")

user_table_insert = ("""INSERT INTO users(user_id,first_name,last_name,gender,level) VALUES(%s,%s,%s,%s,%s)\
                        ON CONFLICT(user_id) DO UPDATE SET level = EXCLUDED.level""")

song_table_insert = ("""INSERT INTO songs(song_id,title,artist_id,year,duration) VALUES(%s,%s,%s,%s,%s) ON CONFLICT DO NOTHING""")

artist_table_insert = ("""INSERT INTO artists(artist_id,name,location,latitude,longitude) VALUES(%s,%s,%s,%s,%s) ON CONFLICT DO NOTHING""")

time_table_insert = ("""INSERT INTO time(start_time, hour, day, week, month, year, weekday) VALUES(%s,%s,%s,%s,%s,%s,%s) ON CONFLICT DO NOTHING """)

# FIND SONGS

song_select = ("""SELECT s.song_id as songid,a.artist_id as artistid from songs s join artists a on s.artist_id=a.artist_id where trim(title)=(%s) and trim(name)=(%s) and duration=(%s)""")

# QUERY LISTS

create_table_queries = [songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
