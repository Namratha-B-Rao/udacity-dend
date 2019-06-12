#### **Objective**
>  
> The **Sparkify** database is designed to assist a start up,Sparkify in organizing their data, they have been collecting on songs and user activity on their new streaming app and to enable them optimize their queries on song play analysis like in understanding what are the popular songs and artists.  
> We have designed a database schema and an ETL pipeline to help them acheive and ease their analytic work.


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
> 1. __users__ - records the users in the app from the respective staged data.  
>_Schema includes_:
>  - user_id, first_name, last_name, gender, level
> 2. __songs__ - records the songs from staged song dataset.  
>_Schema includes_:
>  - song_id, title, artist_id, year, duration
> 3. __artists__ - records the artists from the staged song dataset.
>_Schema includes_:
>  - artist_id, name, location, lattitude, longitude
> 4. __time__ - records timestamps of records in songplays broken down into specific units captured on the logs.
>_Schema includes_
>  - start_time, hour, day, week, month, year, weekday

>##### **ETL design:**

>Post the DB and above related table schemas are created ,The ETL script connects to the Sparkify redshift database, loads log_data and song_data into staging tables, and transforms them into the five tables above, below actions are performed as a part of ETL in brief: (Script **etl.py**)  

>- Function **load_staging_tables** performs the loads of the staging tables for the log_data and song_data respectively.
>- Function **insert_tables** performs the required transformations and inserts the data into the above fact and dimension tables .


#### **Steps to the run the project and associated files created:**

>1.**Cluster_create_&attaching_policy.ipynb** : Contains the relative IAC code to create the redshift cluster and create an IAM role that has read access to S3. Post the same , the redshift DB and the IAM role info is added to the *dwh.cfg* file
>2. **sql_queries.py** : The code for required fact and dimesion tables creation and deletion, the load to the staging tables on the song_data and events_Data, and the relative insertion to the respective tables is written.
>3. **create_tables.py** : Drops and recreates the **_sparkify_** database , establishes connection and creates a cursor. It internally calls in the drop tables and create tables functions per the code defined in the _sql_queries.py_.  
Command to execute on the terminal:**python create_tables.py**
>4. **etl.py** : Implements the above defined ETL design by loading the staged tables and performs the required transformations and insertion to the fact and dimension tables.
Command to execute on the terminal:**python etl.py**



>**NOTE**: **create_tables.py** will have to be executed at least once to create the sparkifydb database to which the other files connect to, before executing other scripts. Also the KEY and SECRET parameter values are secure hence, have deleted the values of my respective AWS key and secret id values in the *dwh.cfg* prior to submission as they are private.

