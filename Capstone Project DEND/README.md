#### **Scope/Objective**
>  
> The scope of the project is to provide an analytic platform by organizing the US immigration data to help them assess the migration trends to the country to identify the length of stay of the non-immigrants and what kind of occupation these migrants are taking up within the country in order to help them assess that there are no illegal stay and activties happening within the country. 
> A Database schema and an ETL pipeline has been designed to help the embassy acheive and ease their analytic work.

#### **Steps Taken**
> 1. Source data : Below data is extracted and cleaned and staged in S3 before further processing:
>    - I94 Immigration Data : This data comes from the US National Tourism and Trade Office. This holds close to 3 million records ( Count = 3,096,093) 
       and is in the SAS data format
>    - World Temperature Data : The data is pulled from Kaggle.This is in the CSV format. Count = 8,599,212 . But the data set is filtered to assess the details only from US.
>    - U.S. City Demographic Data: The data is pulled from Open soft and is extracted and staged as CSV. Count=2891
>    - Airport Code Table : This data holds the airport codes and the corresponding cities. Count =54896
>2. The extracted data is then cleaned using pandas to update the data set to contain atomic values and eliminate the duplicates and the invalid data and staged in S3 by converting them to CSV format.
>3. The respective Fact and Dimesion tables are created using Postgres through Redshift cluster and the ETL is perfomed using Airflow to provide final design below.

#### **Technology used**
> 1. Extraction and data staging : Pandas and S3
> 2. ETL is performed using AIRFLOW which internally connects to the Redshift Cluster and Postgres and performs the relative ETL activities within the cluster.
     The Choice is made because:
     - Using Redshift eases to run aggregations on hundreds of millions of rows in a few seconds. Dealing with close to 3 million records this made a perfect option.
     - AWS works great if you are already on it.
     - It's SQL - so you can pretty much cut and carve your data as you like. The fork of postgres only takes a few commands out.

#### **Design**
>
>##### **Database Schema design:**
>
>Optimized star schema under Dendproject DB includes below:
>###### **_Fact Table_**
>1. __immigration__ : records in logged data associated with migrations to the US.   
>_Schema includes_:
>  - iid, nmid, airportcode, fltno, airportname, arrdate, depdate, length_of_stay, occupation, visatype, native_code, mode

>###### **_Dimension Tables_**
> 1. __airports__ - records the airport details and the corresponding cities from the respective staged data.  
>_Schema includes_:
>  - aiportcode, name, type, country, municipality, gps_code, elevation_ft
> 2. __non_immigrants__ - records the non-immigrant details from the staged data.    
>_Schema includes_:
>  - nmid, age, gender, biryear, occupation, visatype, address, admnum, native_code
> 3. __flights__ - records the flight info of the migrants from the staged dataset.    
>_Schema includes_:
>  - flightid, airline, fltno, arrdate, depdate, mode
> 4. __demographics__ - records the relative demographics of the US.    
>_Schema includes_
>  - dt, city, state, state_code, latitude, longitude, race, male_population, female_population, total_population, foreign_born

>##### **ETL design: (Scripts under airflow folder)**

>Post the DB and above related table schemas are created ,The ETL script connects to the Dendproject redshift database through Airflow, loads the respective staged data in S3 Buckets into staging tables, and transforms them into the five tables above, below actions are performed as a part of ETL in brief:

>- **Operators Created:**
>    - **stg_red_shift.py** : Performs the copy of the stage data from the respetive S3 Bucket to the staging tables.
>    - **load_fact.py** : Performs the respective transformation and loads the fact table
>    - **load_dimension.py** : Performs the respective transformation and loads the dimension tables
>    - **data_quality.py** : Performs the required data quality checks post the load.
>- Script **sql_queries.py** includes the table creation and the transformation code to load the fact and the dimensions table.
>- Dag script **dend_project.py** include the ETL Airflow code with the relative dependencies included for the action to be performed.

>**NOTE**: Image **immigration_ETL.JPG** has the image of the ETL pipeline execution and the success.  

#### **Addressing Other Scenarios**

> **1.If the data was increased by 100x** : To handle bulk load have incorporated the COPY command on the staging operator instead of the INSERT to stage the data using Postgres into the Cluster , so even with the data increase the staging and the ETL will be handled.  

> **2.The pipelines would be run on a daily basis by 7 am every day** : Have set a schedule_interval on the DAG to be triggered at 7:00 am daily,  
>    dag = DAG('dend_project',  
          catchup=False,  
          default_args=default_args,  
          description='Load and transform data in Redshift with Airflow',  
          **schedule_interval**='0 7 * * *'  
        )  
> So irrespective of the changes , the pipeline has been scheduled to run daily an 7:00 am.  

> **3.The database needed to be accessed by 100+ people** : The entire ETL connects to the Redshift cluster which allows and suits best for parallel processing and having their respective access key and secret key any number of users would be able to access the DB as it has been allocated as the public schema.


#### **Details on the folders/files**

> **1. airflow** : Holds the whole ETL code  
> **2. Data Dictionary DEND.pdf** : Reflects the Data dictionary created  
> **3. immigration_ETL.JPG** : Show the ETL design and the success job state of the ETL run made  



