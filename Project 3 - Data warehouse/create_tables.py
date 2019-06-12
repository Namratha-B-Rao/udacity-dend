import configparser
import psycopg2
from sql_queries import create_table_queries, drop_table_queries

# Dropping the required tables before their creation in Redshift DB

def drop_tables(cur, conn):
    '''
    The function drops the required tables prior to their creation.
    Arguments:
        cur: DB cursor
        conn: established connection to Redshift DB
        Return: None
    '''
    for query in drop_table_queries:
        cur.execute(query)
        conn.commit()

# Creation of the required tables
def create_tables(cur, conn):
    '''
    The function creates the required staging,fact and dimension tables
    Arguments:
        cur: DB cursor
        conn: established connection to Redshift DB
        Return: None
    '''
    for query in create_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    # Reading the config file to pull in the required parameter values
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    # Establishing connection to Redshift DB
    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()

    # Function call to the dropping and creation od the required tables within the Redshift DB
    drop_tables(cur, conn)
    create_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()