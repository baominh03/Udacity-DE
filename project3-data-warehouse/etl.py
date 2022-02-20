import configparser
import psycopg2
import sql_queries_nodist
import sql_queries_dist
from time import time

# Run local
config_file_path = './project3-data-warehouse/dwh.cfg'
# Run on Udacity workspace
# config_file_path = 'dwh.cfg'
config = configparser.ConfigParser()
config.read(config_file_path)

LOG_DATA = config.get('S3', 'LOG_DATA')
LOG_JSONPATH = config.get('S3', 'LOG_JSONPATH')
SONG_DATA = config.get('S3', 'SONG_DATA')

# autofill after running create_cluster.py
DWH_ROLE_ARN = config.get("IAM_ROLE", "DWH_ROLE_ARN")


def load_staging_tables(cur, conn, copy_table_queries, schema):
    """
    To Copy data from S3 Bucket to Staging tables

    INPUTS: 
    * cur
    * conn
    * copy_table_queries: query list
    * schema: nodist / dist
    """
    loadTimes = []
    cur.execute('SET search_path TO {};'.format(schema))
    conn.commit()
    print('=== Set Schema: {}'.format(schema))
    print('=== {} staging tables found need to be load ==='.format(
        len(copy_table_queries)))
    for i, query in enumerate(copy_table_queries, 1):
        t0 = time()
        print(
            query + '\n\nLoading staging tables [{}].... please wait'.format(i))
        cur.execute(query)
        conn.commit()
        loadTime = time()-t0
        loadTimes.append(loadTime)
        print('Loaded staging table in: {0:.2f} sec'.format(loadTime))
    print('=== Load staging: Done for schema [{}]'.format(schema))

    # write load time to dwh.cfg
    config.read(config_file_path)
    config.set(schema.upper(), 'LOAD_TIME_STAGING_EVENTS', str(loadTimes[0]))
    config.set(schema.upper(), 'LOAD_TIME_STAGING_SONGS', str(loadTimes[1]))
    with open(config_file_path, 'w') as configfile:
        config.write(configfile)


def insert_tables(cur, conn, insert_table_queries, schema):
    """
    To insert data from Staging tables to tables

    INPUTS: 
    * cur
    * conn
    * insert_table_queries: query list
    * schema: nodist / dist
    """
    cur.execute('SET search_path TO {};'.format(schema))
    conn.commit()
    print('=== Set Schema: {}'.format(schema))
    print('=== {} tables found need to be inserted ==='.format(
        len(insert_table_queries)))
    for i, query in enumerate(insert_table_queries, 1):
        t0 = time()
        print(query + '\nInserting table [{}].... please wait'.format(i))
        cur.execute(query)
        conn.commit()
        print('Inserted table in: {0:.2f} sec'.format(time()-t0))
    print('=== Insert tables: Done for schema [{}]'.format(schema))


def main():
    #Conenct AWS redshift
    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(
        *config['CLUSTER'].values()))
    conn.autocommit = True
    cur = conn.cursor()

    load_staging_tables(
        cur, conn, sql_queries_nodist.copy_table_queries, 'nodist')
    insert_tables(cur, conn, sql_queries_nodist.insert_table_queries, 'nodist')

    load_staging_tables(
        cur, conn, sql_queries_dist.copy_table_queries, 'dist')
    insert_tables(cur, conn, sql_queries_dist.insert_table_queries, 'dist')

    conn.close()


if __name__ == "__main__":
    main()
