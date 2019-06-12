#### **Objective**
>  
> The **Sparkify** database is designed to assist a start up,Sparkify in organizing their data, they have been collecting on songs and user activity on their new streaming app and to enable them optimize their queries on song play analysis like in understanding what are the popular songs and artists.  
> We have designed a database schema and an ETL pipeline to help them acheive and ease their analytic work. With their growing data, Sparkify wants to move their data from data warehouse to data lakes, The ETL designed will extract their data from S3, process them using Spark, and load the data back into S3 as a set of dimensional tables.


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

>The ETL script is designed extract the Sparkify data from S3,process them using Spark and load the data back to S3 as a set of 5 dimensional tables, below actions are performed as a part of ETL in brief: (Script **etl.py**)  

>- Function **process_song_data** performs the extraction of songs data from S3 and creation and loading of songs and artists dimensional table data back to S3.
>- Function **process_log_data** performs the extraction of logs data from S3 and creating and loading of users,time and songplays table back to S3

#### **Steps to the run the project and associated files created:**

>1.**ETLwithsample.ipynb** : Contains the ETL trial made with sample data and the required KEY and SECRET ID deatils for AWS are provided in the *dl.cfg* file
>2. **etl.py** : Implements the above defined ETL design by extracting the related data from S3 transforming and loading them back to S3 partitioned per requirement.
Command to execute on the terminal:**python etl.py**


>**NOTE**: the KEY and SECRET parameter values are secure hence, have deleted the values of my respective AWS key and secret id values in the *dl.cfg* prior to submission as they are private.

