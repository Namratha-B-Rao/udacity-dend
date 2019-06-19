#### **Objective**
>  
> The **Sparkify** database is designed to assist a start up,Sparkify in organizing their data, they have been collecting on songs and user activity on their new streaming app and to enable them optimize their queries on song play analysis like in understanding what are the popular songs and artists.  
> We have designed a database schema and an ETL pipeline using Casandra to help them acheive the same.



#### **Design**
>
>The tables are modelled to query below from the events data in Casandra,
>1. To collect the artist, song title and song's length in the music app history that was heard during  sessionId = 338, and itemInSession  = 4
>**Query used:** select * from music_library where sessionId = 338 and itemInSession = 4
>2. To list only the following: name of artist, song (sorted by itemInSession) and user (first and last name) for userid = 10, sessionid = 182
>**Query used:** select artist_name,song,first_name,last_name from artist_library where user_id = 10 and sessionId = 182
>3. To list every user name (first and last) in my music app history who listened to the song 'All Hands Against His Own'
>**Query used:** select first_name,last_name from user_library where song = 'All Hands Against His Own'  
   






