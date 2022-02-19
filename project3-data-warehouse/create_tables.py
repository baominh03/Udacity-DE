import configparser
import psycopg2
from sql_queries import create_table_queries, drop_table_queries

# Run local
config_file_path = './project3-data-warehouse/dwh.cfg'
# Run on Udacity workspace
# config_file_path = 'dwh.cfg'

def drop_tables(cur, conn):
    for query in drop_table_queries:
        cur.execute(query)
        conn.commit()
    print('=== Dropped tables if exists ===')


def create_tables(cur, conn):
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

    drop_tables(cur, conn)
    
    create_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()