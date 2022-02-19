import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries
from time import time

## Run local
config_file_path = './project3-data-warehouse/dwh.cfg'
## Run on Udacity workspace
# config_file_path = 'dwh.cfg'


def load_staging_tables(cur, conn):
    print('=== {} staging tables found need to be load ==='.format(len(copy_table_queries)))
    for i, query in enumerate(copy_table_queries, 1):
        t0 = time()
        print(query + '\n\nLoading staging tables [{}].... please wait'.format(i))
        cur.execute(query)
        conn.commit()
        print('Loaded staging table in: {0:.2f} sec'.format(time()-t0))
    print('=== Load staging: Done')


def insert_tables(cur, conn):
    print('=== {} tables found need to be inserted ==='.format(len(insert_table_queries)))
    for i, query in enumerate(insert_table_queries, 1):
        t0 = time()
        print(query + '\nInserting table [{}].... please wait'.format(i))
        cur.execute(query)
        conn.commit()
        print('Inserted table in: {0:.2f} sec'.format(time()-t0))
    print('=== Insert tables: Done')


def main():
    config = configparser.ConfigParser()
    config.read(config_file_path)
    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()
    
    load_staging_tables(cur, conn)
    
    insert_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()