#Importing required packages

from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.postgres_operator import PostgresOperator
from airflow.operators import (
    StageToRedshiftOperator, 
    LoadFactOperator,
    LoadDimensionOperator, 
    DataQualityOperator
)
from helpers import SqlQueries

#Setting default arguments
default_args = {
    'owner' 'udacity',
    'start_date' datetime(2019, 1, 12),
    'depends_on_past' False,
    'retries'  3,
    'retry_delay' timedelta(minutes=5),
    'email_on_retry' False
}

#DAG initialization
dag = DAG('dend_project',  
          catchup=False,  
          default_args=default_args,  
          description='Load and transform data in Redshift with Airflow',  
          schedule_interval='0 7   '  
        )  

#Start trigger
start_operator = DummyOperator(task_id='Begin_execution',  dag=dag)

#Call to Staging to Redshift Operator
stage_immigration_to_redshift = StageToRedshiftOperator(
    task_id='Stage_immigration',
    dag=dag,
    table=staging_immigration,
    redshift_conn_id=redshift,
    aws_credentials_id=aws_credentials,
    file_typ=csv,
    s3_bucket=dend-project,
    s3_key=immigration_data,
    sql=SqlQueries.create_immigration_staging_table,
    delimiter=,
)

stage_temperature_to_redshift = StageToRedshiftOperator(
    task_id='Stage_temperature',
    dag=dag,
    table=staging_temperature,
    redshift_conn_id=redshift,
    aws_credentials_id=aws_credentials,
    file_typ=csv,
    s3_bucket=dend-project,
    s3_key=temperature_data,
    sql=SqlQueries.create_temperature_staging_table,
    delimiter=,
)

stage_usdemographics_to_redshift = StageToRedshiftOperator(
    task_id='Stage_usdemographics',
    dag=dag,
    table=staging_usdemographics,
    redshift_conn_id=redshift,
    aws_credentials_id=aws_credentials,
    file_typ=csv,
    s3_bucket=dend-project,
    s3_key=us_demographics_data,
    sql=SqlQueries.create_usdemographics_staging_table,
    delimiter=,
)

stage_airportcodes_to_redshift = StageToRedshiftOperator(
    task_id='Stage_airportcodes',
    dag=dag,
    table=staging_airportcodes,
    redshift_conn_id=redshift,
    aws_credentials_id=aws_credentials,
    file_typ=csv,
    s3_bucket=dend-project,
    s3_key=airport_codes_data,
    sql=SqlQueries.create_airportcodes_staging_table,
    delimiter=,
)

#Call to Loading the Dimension table operator
load_airports_dimension_table = LoadDimensionOperator(
    task_id='Load_airports_dim_table',
    dag=dag,
    redshift_conn_id=redshift,
    aws_credentials_id=aws_credentials,
    create_table_sql=SqlQueries.create_airports_table,
    insert_table_sql=SqlQueries.airports_table_insert,
    mode=overwrite,
    target_table=airports
)

load_non_immigrants_dimension_table = LoadDimensionOperator(
    task_id='Load_non_immigrants_dim_table',
    dag=dag,
    redshift_conn_id=redshift,
    aws_credentials_id=aws_credentials,
    create_table_sql=SqlQueries.create_non_immigrants_table,
    insert_table_sql=SqlQueries.non_immigrants_table_insert,
    mode=overwrite,
    target_table=non_immigrants
)

load_flights_dimension_table = LoadDimensionOperator(
    task_id='Load_flights_dim_table',
    dag=dag,
    redshift_conn_id=redshift,
    aws_credentials_id=aws_credentials,
    create_table_sql=SqlQueries.create_flights_table,
    insert_table_sql=SqlQueries.flights_table_insert,
    mode=overwrite,
    target_table=flights
)

load_demographics_dimension_table = LoadDimensionOperator(
    task_id='Load_demographics_dim_table',
    dag=dag,
    redshift_conn_id=redshift,
    aws_credentials_id=aws_credentials,
    create_table_sql=SqlQueries.create_demographics_table,
    insert_table_sql=SqlQueries.demographics_table_insert,
    mode=overwrite,
    target_table=demographics
)

#Call to Loading the Dimension table operator

load_immigration_table = LoadFactOperator(
    task_id='Load_immigration_fact_table',
    dag=dag,
    redshift_conn_id=redshift,
    aws_credentials_id=aws_credentials,
    create_table_sql=SqlQueries.create_immigration_table,
    insert_table_sql=SqlQueries.immigration_table_insert,
    mode=append,
    target_table=immigration
    )

#Data Quality Checks
run1_quality_checks = DataQualityOperator(
    task_id='Run1_data_quality_checks',
    dag=dag,
    redshift_conn_id=redshift,
    table_name=immigration
)

run2_quality_checks = DataQualityOperator(
    task_id='Run2_data_quality_checks',
    dag=dag,
    redshift_conn_id=redshift,
    table_name=flights
)

# ETL end trigger
end_operator = DummyOperator(task_id='Stop_execution',  dag=dag)

# Setting of the ETL dependencies
start_operator  stage_immigration_to_redshift
start_operator  stage_temperature_to_redshift
start_operator  stage_usdemographics_to_redshift
start_operator  stage_airportcodes_to_redshift
stage_airportcodes_to_redshiftload_immigration_table
stage_immigration_to_redshiftload_immigration_table
stage_temperature_to_redshift  load_immigration_table
stage_usdemographics_to_redshift load_immigration_table

load_non_immigrants_dimension_table  load_immigration_table
load_demographics_dimension_table  load_immigration_table
load_airports_dimension_table  load_immigration_table
load_flights_dimension_table  load_immigration_table

load_non_immigrants_dimension_table  run1_quality_checks
load_demographics_dimension_table run1_quality_checks
load_airports_dimension_table  run1_quality_checks
load_flights_dimension_table  run1_quality_checks

load_non_immigrants_dimension_table  run2_quality_checks
load_demographics_dimension_table run2_quality_checks
load_airports_dimension_table  run2_quality_checks
load_flights_dimension_table  run2_quality_checks

run1_quality_checks  end_operator
run2_quality_checks  end_operator