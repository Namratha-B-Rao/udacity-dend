#!/usr/bin/env python
# coding: utf-8

# # Part I. ETL Pipeline for Pre-Processing the Files

# ## PLEASE RUN THE FOLLOWING CODE FOR PRE-PROCESSING THE FILES

# #### Import Python packages 

# In[55]:


# Importing Python packages 
import pandas as pd
import cassandra
import re
import os
import glob
import numpy as np
import json
import csv


# #### Creating list of filepaths to process original event csv data files

# In[56]:


# checking the current working directory
print(os.getcwd())

# Getting the current folder and subfolder event data
filepath = os.getcwd() + '/event_data'

# For loop to create a list of files and collect each filepath
for root, dirs, files in os.walk(filepath):
    
# Joining the file path and roots with the subdirectories using glob
    file_path_list = glob.glob(os.path.join(root,'*'))
    #print(file_path_list)


# #### Processing the files to create the data file csv that will be used for Apache Casssandra tables

# In[58]:


# initiating an empty list of rows that will be generated from each file
full_data_rows_list = [] 
    
# for every filepath in the file path list 
for f in file_path_list:

# reading csv file 
    with open(f, 'r', encoding = 'utf8', newline='') as csvfile: 
        # creating a csv reader object 
        csvreader = csv.reader(csvfile) 
        next(csvreader)
        
 # extracting each data row one by one and append it        
        for line in csvreader:
            #print(line)
            full_data_rows_list.append(line) 
            
# Printing the total number of rows 
print(len(full_data_rows_list))

# creating a smaller event data csv file called event_datafile_full csv that will be used to insert data into the \
# Apache Cassandra tables
csv.register_dialect('myDialect', quoting=csv.QUOTE_ALL, skipinitialspace=True)

with open('event_datafile_new.csv', 'w', encoding = 'utf8', newline='') as f:
    writer = csv.writer(f, dialect='myDialect')
    writer.writerow(['artist','firstName','gender','itemInSession','lastName','length',                'level','location','sessionId','song','userId'])
    for row in full_data_rows_list:
        if (row[0] == ''):
            continue
        writer.writerow((row[0], row[2], row[3], row[4], row[5], row[6], row[7], row[8], row[12], row[13], row[16]))


# In[59]:


# checking the total number of rows in the generated csv file
with open('event_datafile_new.csv', 'r', encoding = 'utf8') as f:
    print(sum(1 for line in f))


# # Part II. Complete the Apache Cassandra coding portion of your project. 
# 
# ## Now you are ready to work with the CSV file titled <font color=red>event_datafile_new.csv</font>, located within the Workspace directory.  The event_datafile_new.csv contains the following columns: 
# - artist 
# - firstName of user
# - gender of user
# - item number in session
# - last name of user
# - length of the song
# - level (paid or free song)
# - location of the user
# - sessionId
# - song title
# - userId
# 
# The image below is a screenshot of what the denormalized data should appear like in the <font color=red>**event_datafile_new.csv**</font> after the code above is run:<br>
# 
# <img src="images/image_event_datafile_new.jpg">

# ## Begin writing your Apache Cassandra code in the cells below

# #### Creating a Cluster

# In[60]:


# Establishing a connection to a Cassandra instance on the local machine 
# (127.0.0.1)

from cassandra.cluster import Cluster
cluster = Cluster()

# Opening a session ,to establish connection and begin executing queries
session = cluster.connect()


# #### Create Keyspace

# In[61]:


# Creation of Keyspace to create the tables
try:
    session.execute("""
    CREATE KEYSPACE IF NOT EXISTS sparkify 
    WITH REPLICATION = 
    { 'class' : 'SimpleStrategy', 'replication_factor' : 1 }"""
)

except Exception as e:
    print(e)


# #### Set Keyspace

# In[62]:


# Setting the KEYSPACE to the keyspace specified above
try:
    session.set_keyspace('sparkify')
except Exception as e:
    print(e)


# ### Now we need to create tables to run the following queries. Remember, with Apache Cassandra you model the database tables on the queries you want to run.

# ## Create queries to ask the following three questions of the data
# 
# ### 1. Give me the artist, song title and song's length in the music app history that was heard during  sessionId = 338, and itemInSession  = 4
# 
# 
# ### 2. Give me only the following: name of artist, song (sorted by itemInSession) and user (first and last name) for userid = 10, sessionid = 182
#     
# 
# ### 3. Give me every user name (first and last) in my music app history who listened to the song 'All Hands Against His Own'
# 
# 
# 

# In[63]:


## Creation of table modelled to pull the artist, song title and song's length in the music app history that was heard during \
## sessionId = 338, and itemInSession = 4

query = "CREATE TABLE IF NOT EXISTS music_library"
query = query + "(sessionId int,itemInSession int,artist_name varchar, song varchar, length float,PRIMARY KEY (sessionId,itemInSession))"
try:
    session.execute(query)
except Exception as e:
    print(e)
                    


# In[64]:


# Setting up of the CSV file containing the data to be inserted
file = 'event_datafile_new.csv'

with open(file, encoding = 'utf8') as f:
    csvreader = csv.reader(f)
    next(csvreader) # skipping header
    for line in csvreader:
## Assigning the INSERT statements into the `query` variable and inserting the data to the table
        query = "INSERT INTO music_library(sessionId, itemInSession, artist_name, song, length)"
        query = query + "VALUES(%s, %s, %s, %s, %s)"
        session.execute(query, (int(line[8]), int(line[3]), line[0], line[9], float(line[5]) ))


# #### Do a SELECT to verify that the data have been inserted into each table

# In[65]:


## SELECT statement to verify the data was entered into the table and pull the results for Query 1
query = "select * from music_library where sessionId = 338 and itemInSession = 4"
try:
    rows = session.execute(query)
    #print(rows[0])
except Exception as e:
    print(e)
    
for row in rows:
    print (row.artist_name,'|',row.song,'|',row.length)
    
    


# ### COPY AND REPEAT THE ABOVE THREE CELLS FOR EACH OF THE THREE QUESTIONS

# In[67]:


## Creation of table modelled to pull the name of artist, song (sorted by itemInSession) and user (first and last name)\
## for userid = 10, sessionid = 182
query = "CREATE TABLE IF NOT EXISTS artist_library"
query = query + "(user_id int, sessionId int,  itemInSession int, artist_name varchar, song varchar, first_name text, last_name text, PRIMARY KEY (user_id,sessionId,song,itemInSession))"
try:
    session.execute(query)
except Exception as e:
    print(e)

                    


# In[68]:


# Setting up of the CSV file containing the data to be inserted
file = 'event_datafile_new.csv'

with open(file, encoding = 'utf8') as f:
    csvreader = csv.reader(f)
    next(csvreader) # skip header
    for line in csvreader:
## Assigning the INSERT statements into the `query` variable and inserting the data to the table
        query = "INSERT INTO artist_library(user_id, sessionId, song, itemInSession, artist_name, first_name, last_name)"
        query = query + "VALUES(%s, %s, %s, %s, %s, %s, %s)"
        session.execute(query, (int(line[10]), int(line[8]), line[9], int(line[3]), line[0], line[1], line[4]))


# In[47]:


## SELECT statement to verify the data was entered into the table and pull the results for Query 2
query = "select artist_name,song,first_name,last_name from artist_library where user_id = 10 and sessionId = 182 "
try:
    rows = session.execute(query)
    #print(rows[0])
except Exception as e:
    print(e)
    
for row in rows:
    print (row.artist_name,'|', row.song,'|',row.first_name,row.last_name)
    


# In[69]:


## Creation of table modelled to pull every user name (first and last) in music app history who listened to the song 'All Hands Against His Own'
query = "CREATE TABLE IF NOT EXISTS user_library"
query = query + "(song varchar, user_id int, first_name text, last_name text, PRIMARY KEY (song, user_id))"
try:
    session.execute(query)
except Exception as e:
    print(e)

                    


# In[70]:


# Setting up of the CSV file containing the data to be inserted
file = 'event_datafile_new.csv'

with open(file, encoding = 'utf8') as f:
    csvreader = csv.reader(f)
    next(csvreader) # skip header
    for line in csvreader:
## Assigning the INSERT statements into the `query` variable and inserting the data to the table
        query = "INSERT INTO user_library(song, user_id,first_name, last_name)"
        query = query + "VALUES(%s, %s, %s, %s)"
        session.execute(query, (line[9], int(line[10]), line[1], line[4]))


# In[71]:


## SELECT statement to verify the data was entered into the table and pull the results for Query 3
query = "select first_name,last_name from user_library where song = 'All Hands Against His Own' "
try:
    rows = session.execute(query)
    #print(rows[0])
except Exception as e:
    print(e)
    
for row in rows:
    print (row.first_name,row.last_name)


# ### Drop the tables before closing out the sessions

# In[72]:


## Dropping the created tables before closing out the sessions
try:
    session.execute("drop table if exists music_library;")
except Exception as e:
    print(e)

try:
    session.execute("drop table if exists artist_library;")
except Exception as e:
    print(e)

try:
    session.execute("drop table if exists user_library;")
except Exception as e:
    print(e)


# ### Close the session and cluster connection¶

# In[73]:


session.shutdown()
cluster.shutdown()


# In[ ]:





# In[ ]:




