#### **Objective**
>  
> The **Sparkify** database is designed to assist a start up,Sparkify in organizing their data, they have been collecting on songs and user activity on their new streaming app and to enable them optimize their queries on song play analysis like in understanding what are the popular songs and artists.  
> We have designed a database schema and an ETL pipeline to help them acheive the same.



#### **Design**
>
>##### **Database Schema design:**
>
>Optimized star schema under Sparkify DB includes below:
>###### **_Fact Table_**
>1. __songplays__ : records in logged data associated with song plays i.e. records with page NextSong.   
>_Schema includes_:
>  - songplay_id, start_time, user_id, level, song_id, artist_id, session_id, location, user_agent

>###### **_Dimension Tables_**
> 1. __users__ - records the users in the app from the log data.  
>_Schema includes_:
>  - user_id, first_name, last_name, gender, level
> 2. __songs__ - records the songs from song dataset.  
>_Schema includes_:
>  - song_id, title, artist_id, year, duration
> 3. __artists__ - records the artists from the song dataset.
>_Schema includes_:
>  - artist_id, name, location, lattitude, longitude
> 4. __time__ - records timestamps of records in songplays broken down into specific units captured on the logs.
>_Schema includes_
>  - start_time, hour, day, week, month, year, weekday

>##### **ETL design:**

>Post the DB and above related table schemas are created , below ations are performed as a part of ETL in brief: (Script **etl.py**)  
>- Function **process_song_file** reads the songs dataset in json format into a panda data frame and has the records inserted to the **songs** and **artists** dimension tables.
>- Function **process_log_file** reads the logs dataset in json format into a panda data frame , filters the data frame by NextSong action , creates the required time and user df and inserts records to the **time** and **users** dimension tables. It then performs a filter to retrived the song ID and artist ID based on the title, artist name, and duration of a song in the long table and then creates and populates the **songplays** fact table.
>- Function **process_data** directs to the expected data set relative to the above by walking through the data recursively from the root dirs **_data/song_data_** and **_data/log_data_** required to be passed to the above two functions.



#### **Steps to the run the project and associated files created:**

>1. **sql_queries.py** : The code for required fact and dimesion tables creation and deletion, and the relative insertion to the respective table is written.
>2. **create_tables.py** : Drops and recreates the **_sparkify_** database , establishes connection and creates a cursor. It internally calls in the drop tables and create tables functions per the code defined in the _sql_queries.py_.  
Command to execute on the terminal:**python create_tables.py**
>3. **etl.py** : Implements the above defined ETL design and performs the required insertion to the fact and dimension tables.
Command to execute on the terminal:**python etl.py**
>4. **test.ipynb** : Performs the required tests on the tables created and inserted data to ensure on the successful execution on the scripts created.


>**NOTE**: **create_tables.py** will have to be executed at least once to create the sparkifydb database to which the other files connect to, before executing other scripts.

#### **Queries on the data set and observations** :

1. **_The user who has viewed the app most is_** :  
   - %sql SELECT user_id,count(*) as count FROM songplays group by user_id  having count(*) > 1 order by count desc;  
    user_id	count  
    49       689 
   - %sql SELECT * from users where user_id = 49;  
    user_id	first_name	last_name	gender	level  
    49	Chloe	Cuevas	F	paid


2. **_Theres is no match on title, artist name for the song data set and the log data set , hence the song_id and artist_id on the song plays table reflects None: Sample analysis**

  - From Log data:  
    df_check2 = df[df.userId == '4']  
    df_check2.head()  
    
	artist	auth	firstName	gender	itemInSession	lastName	length	level	location	method	page	registration	sessionId	song	status	ts	userAgent	userId  
42	Cedell Davis	Logged In	Alivia	F	0	Terrell	178.36363	free	Parkersburg-Vienna, WV	PUT	NextSong	1.540505e+12	587	Chicken Hawk	200	1543460835796	"Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebK...	4  
171	James Newton Howard	Logged In	Alivia	F	0	Terrell	141.55710	free	Parkersburg-Vienna, WV	PUT	NextSong	1.540505e+12	1054	I'm Sorry	200	1543511176796	"Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebK...	4  

   - From artist table:  

     %sql select * from artists where TRIM(name) IN ('Cedell Davis','James Newton Howard');   
     Returned no records.Hence not joined and artist id reflected as None in Song plays table.
     
   - From song table :
   
     %sql select * from songs where trim(title) in ('Chicken Hawk');  
     Returned no records.Hence song _id refected as None in Song plays table.
 







