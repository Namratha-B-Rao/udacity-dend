import configparser
from datetime import datetime
import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format
from pyspark.sql.functions import monotonically_increasing_id


config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID']=config['AWS']['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY']=config['AWS']['AWS_SECRET_ACCESS_KEY']


def create_spark_session():
    '''
    The function returns a spark session
    '''
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark


def process_song_data(spark, input_data, output_data):
    '''
    The function extracts the song data from S3 transforms per required and loads it back to S3
    Arguments:
        spark: Spark session created
        input_data: S3 bucket to extract from
        output_data: S3 bucket to load back to
    '''
    
    #  defining filepath to song data file
    print(" Song data Read Start")
    song_data = "s3a://udacity-dend/song_data/*/*/*/*.json"
    #song_data = "s3a://udacity-dend/song_data/A/B/C/TRABCEI128F424C983.json" 
    
    # reading song data file
    df = spark.read.json(song_data)
    print("Song data Read End")

    # extracting columns to create songs table
    print("Song table extract start")
    songs_table = df.select("song_id","title","artist_id","year","duration").dropDuplicates()
    
    # writing songs table to parquet files partitioned by year and artist
    print("Songs table write to S3 start")
    songs_table.write.partitionBy("year", "artist_id").mode('Overwrite').parquet("s3a://datalakeproj/songs/")
    print("Song table write complete")

    # extracting columns to create artists table
    print("Artist table extract start")
    artists_table = df.selectExpr("artist_id", "artist_name as name", "artist_location as location", "artist_latitude as latitude", "artist_longitude as longitude").dropDuplicates()
    
    # writing artists table to parquet files
    print("Artist table write to S3 start")
    artists_table.write.mode('overwrite').parquet("s3a://datalakeproj/artists/")
    print("Artist table write complete")


def process_log_data(spark, input_data, output_data):
    '''
    The function extracts the logs data from S3 transforms per required and loads it back to S3
    Arguments:
        spark: Spark session created
        input_data: S3 bucket to extract from
        output_data: S3 bucket to load back to
    '''
    # Setting filepath to log data file  
    print("Log data Read start")
    log_data = "s3a://udacity-dend/log_data/*/*/*.json"
    #log_data = "s3a://udacity-dend/log_data/2018/11/2018-11-12-events.json"

    # reading log data file
    dfLog = spark.read.json(log_data) 
    print("Log data Read complete")
    
    # filtering by actions for song plays
    print("Filter for song plays")
    dfLog = dfLog.filter(dfLog.page == 'NextSong')

    # extracting columns for users table  
    print("User table extract start")
    users_table = dfLog.selectExpr("userId as user_id", "firstName as first_name", "lastName as last_name", "gender", "level").dropDuplicates()
    
    # writing users table to parquet files
    print("User table write to S3 start")
    users_table.write.mode('overwrite').parquet("s3a://datalakeproj/users/")
    print("User table write complete") 
    
    # creating datetime column from original timestamp column
    get_datetime = udf(lambda x: datetime.datetime.fromtimestamp(x / 1000).strftime('%Y-%m-%d %H:%M:%S'))
    dfLogFinal = dfLog.withColumn("date_time", get_datetime(dfLog.ts))
    dfLogFinal.createOrReplaceTempView("logs_staging")
    
    # extracting columns to create time table
    print("Time table Extract Start ")
    time_table = dfLogFinal.selectExpr("date_time as start_time",\
              "hour(date_time) as hour", \
              "dayofmonth(date_time) as day",\
              "weekofyear(date_time) as week",\
              "month(date_time) as month",\
              "year(date_time) as year",\
              "date_format(date_time,'u') as weekday").dropDuplicates()
    
    # writing time table to parquet files partitioned by year and month
    print("Time table write to S3 start")
    time_table.write.partitionBy("year", "month").mode('Overwrite').parquet("s3a://datalakeproj/time/")
    print("Time table write complete")

    # reading in song data to use for songplays table
    
    song_data = "s3a://udacity-dend/song_data/*/*/*/*.json"
    #song_data = "s3a://udacity-dend/song_data/A/B/C/TRABCEI128F424C983.json" 
    df = spark.read.json(song_data)
    df.createOrReplaceTempView("songs_staging")
    
    # extracting columns from joined song and log datasets to create songplays table 
    print("Sonplays table extract")
    songplays_table = spark.sql('''SELECT distinct e.ts as start_time,\
                                                   e.userId as user_id,\
                                                   e.level as level,\
                                                   s.song_id as song_id,\
                                                   s.artist_id as artist_id,\
                                                   e.sessionId as session_id,\
                                                   e.location as location,\
                                                   e.userAgent as user_agent,\
                                                   year(date_time) as year,\
                                                   month(date_time) as month\
                                                   FROM logs_staging e join songs_staging s\
                                                   ON (e.song = s.title and s.artist_name == e.artist and s.duration = e.length)''')
    
    songplays_table = songplays_table.withColumn("songplay_id",monotonically_increasing_id())

    # writing songplays table to parquet files partitioned by year and month
    print("songplays table write to S3 start")
    songplays_table.write.partitionBy("year", "month").mode('Overwrite').parquet("s3a://datalakeproj/songplays/")
    print("songplays table write complete")

def main():
    spark = create_spark_session()
    input_data = "s3a://udacity-dend/"
    output_data = "s3a://datalakeproj/"
    
    process_song_data(spark, input_data, output_data)    
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()
