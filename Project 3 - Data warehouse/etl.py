import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries

# Staging of the songs and events data
def load_staging_tables(cur, conn):
    '''
    The function loads the staging tables from the respective S3 bucket for the staging_songs and staginglogdata
    Arguments:
        cur: DB cursor
        conn: established connection to Redshift DB
        Return: None
    '''
    for query in copy_table_queries:
        cur.execute(query)
        conn.commit()

# Loading of the respective Fact and Dimension tables
def insert_tables(cur, conn):
    '''
    The function performs the required transformations and inserts the data to the fact and dimesion tables from the staged data
    Arguments:
        cur: DB cursor
        conn: established connection to Redshift DB
        Return: None
    '''
    for query in insert_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    # Reading the config file to pull in the required parameter values
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    # Establishing connection to Redshift DB
    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()
    
    # Staging of the data and load to the respective fact and dimension tables
    load_staging_tables(cur, conn)
    insert_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()