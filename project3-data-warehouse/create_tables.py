import configparser
import psycopg2
import sql_queries_nodist
import sql_queries_dist

# Run local
config_file_path = './project3-data-warehouse/dwh.cfg'
# Run on Udacity workspace
# config_file_path = 'dwh.cfg'

def create_schema(cur, conn, schema):
    """
    Create if not exist schema and set search_path before drop or create table

    INPUTS: 
    * cur
    * conn
    * schema: nodist / dist
    """
    cur.execute('CREATE SCHEMA IF NOT EXISTS {};'.format(schema))
    conn.commit()
    cur.execute('SET search_path TO {};'.format(schema))
    conn.commit()
    print('=== Created Schema [{}] if not exists ==='.format(schema))

def drop_tables(cur, conn, drop_table_queries):
    """
    Drops each table using the queries in `drop_table_queries` list.

    INPUTS: 
    * cur
    * conn
    * drop_table_queries: query list
    """
    for query in drop_table_queries:
        cur.execute(query)
        conn.commit()
    print('=== Dropped tables if exists ===')


def create_tables(cur, conn, create_table_queries):
    """
    Creates each table using the queries in `create_table_queries` list. 

    INPUTS: 
    * cur
    * conn
    * create_table_queries: query list
    """
    for query in create_table_queries:
        cur.execute(query)
        conn.commit()
    print('=== Created tables if not exists ===')


def main():
    config = configparser.ConfigParser()
    config.read(config_file_path)

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()
    print('=== Connected redshift successfully ===\nhost={}\ndbname={}\nuser={}\npassword={}\nport={}'.format(*config['CLUSTER'].values()))

    create_schema(cur, conn, 'nodist')

    drop_tables(cur, conn, sql_queries_nodist.drop_table_queries)
    
    create_tables(cur, conn, sql_queries_nodist.create_table_queries)

    create_schema(cur, conn, 'dist')

    drop_tables(cur, conn, sql_queries_dist.drop_table_queries)
    
    create_tables(cur, conn, sql_queries_dist.create_table_queries)

    conn.close()


if __name__ == "__main__":
    main()