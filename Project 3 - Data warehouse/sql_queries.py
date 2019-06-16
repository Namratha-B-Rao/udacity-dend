import configparser


# Reading the Config file to resolve the respective parameter values
config = configparser.ConfigParser()
config.read('dwh.cfg')

# DROPPING the tables prior to their creation

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs"
songplay_table_drop = "DROP TABLE IF EXISTS songplays"
user_table_drop = "DROP TABLE IF EXISTS users"
song_table_drop = "DROP TABLE IF EXISTS songs"
artist_table_drop = "DROP TABLE IF EXISTS artists"
time_table_drop = "DROP TABLE IF EXISTS time"

# Creation of the staged,fact and dimension tables

staging_events_table_create= ("""CREATE TABLE IF NOT EXISTS staging_events(artist varchar(255),\
                                                                          auth varchar(100),\
                                                                          firstName varchar(100),\
                                                                          gender varchar(2),\
                                                                          itemInSession integer,\
                                                                          lastName varchar(100),\
                                                                          length float ,\
                                                                          level varchar(20) ,\
                                                                          location varchar(100),\
                                                                          method varchar(20),\
                                                                          page varchar(20) ,\
                                                                          registration varchar(70),\
                                                                          sessionId integer sortkey,\
                                                                          song varchar(255),\
                                                                          status integer,\
                                                                          ts bigint,\
                                                                          userAgent varchar(255),\
                                                                          userId integer)""")

staging_songs_table_create = ("""CREATE TABLE IF NOT EXISTS staging_songs(artist_id varchar(255),\
                                                                          artist_latitude varchar(100),\
                                                                          artist_location varchar(255),\
                                                                          artist_longitude varchar(100),\
                                                                          artist_name varchar(255),\
                                                                          duration float,\
                                                                          num_songs integer,\
                                                                          song_id varchar(255),\
                                                                          title varchar(255),\
                                                                          year integer)""")

songplay_table_create = ("""CREATE TABLE IF NOT EXISTS songplays(songplay_id integer IDENTITY(0,1) primary key distkey,\
                                                                 start_time bigint not null,\
                                                                 user_id integer not null,\
                                                                 level varchar(20),\
                                                                 song_id varchar(255) not null,\
                                                                 artist_id varchar(255) not null,\
                                                                 session_id integer not null, \
                                                                 location varchar(100), \
                                                                 user_agent varchar(255))""")

user_table_create = ("""CREATE TABLE IF NOT EXISTS users(user_id integer primary key NOT NULL distkey,\
                                                         first_name varchar(100),\
                                                         last_name varchar(100),\
                                                         gender varchar(2),\
                                                         level varchar(20))""")

song_table_create = ("""CREATE TABLE IF NOT EXISTS songs(song_id varchar(255) primary key NOT NULL distkey,\
                                                         title varchar(255),\
                                                         artist_id varchar(255) NOT NULL,\
                                                         year integer,\
                                                         duration float)""")

artist_table_create = ("""CREATE TABLE IF NOT EXISTS artists(artist_id varchar(255) primary key NOT NULL distkey,\
                                                             name varchar(200),\
                                                             location varchar(255),\
                                                             latitude varchar(100),\
                                                             longitude varchar(100))""")

time_table_create = ("""CREATE TABLE IF NOT EXISTS time(start_time timestamp NOT NULL,\
                                                        hour integer,\
                                                        day integer NOT NULL,\
                                                        week integer ,\
                                                        month integer NOT NULL,\
                                                        year integer NOT NULL,\
                                                        weekday integer,\
                                                        PRIMARY KEY(start_time, day, month, year))""")

# Copying and Loading data to the STAGING TABLES from the respective S3 buckets

staging_events_copy = ("""copy staging_events from {} iam_role {} region 'us-west-2' FORMAT AS JSON {} TRUNCATECOLUMNS blanksasnull emptyasnull;""").format(config.get("S3","LOG_DATA"),config.get("IAM_ROLE","ARN"),config.get("S3","LOG_JSONPATH"))

staging_songs_copy = ("""copy staging_songs from {} iam_role {} region 'us-west-2' FORMAT AS JSON 'auto' TRUNCATECOLUMNS blanksasnull emptyasnull;""").format(config.get("S3","SONG_DATA"),config.get("IAM_ROLE","ARN"))

# Transform and insert to the FINAL TABLES

songplay_table_insert = (""" INSERT INTO songplays (start_time, user_id, level, song_id, artist_id, session_id, location, user_agent) SELECT distinct e.ts as start_time, e.userId as user_id, e.level as level, s.song_id as song_id, s.artist_id as artist_id, e.sessionId as session_id, e.location as location, e.userAgent as user_agent FROM staging_events e join staging_songs s on (trim(s.title) = trim(e.song) and trim(s.artist_name) = trim(e.artist) and s.duration = e.length) where trim(e.page) in ('NextSong');
""")

user_table_insert = (""" INSERT INTO users (user_id, first_name, last_name, gender, level) SELECT distinct userId, firstName, lastName, gender, level FROM staging_events where page in ('NextSong') and userId IS NOT NULL;
""")

song_table_insert = (""" INSERT INTO songs (song_id, title, artist_id, year, duration) SELECT distinct song_id, title, artist_id, year, duration FROM staging_songs;
""")

artist_table_insert = ("""INSERT INTO artists (artist_id, name, location, latitude, longitude) SELECT distinct artist_id, artist_name, artist_location, artist_latitude, artist_longitude FROM staging_songs;
""")

time_table_insert = ("""INSERT INTO time (start_time, hour, day, week, month, year, weekday) SELECT distinct dateadd(s, convert(bigint, ts) / 1000, convert(datetime, '1-1-1970 00:00:00'))::timestamp as start_time, extract(hour from start_time) as hour, extract(day from start_time) as day, extract(week from start_time) as week, extract(month from start_time) as month, extract(year from start_time) as year, extract(dow from start_time) as weekday FROM staging_events;
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
#drop_table_queries = [songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
