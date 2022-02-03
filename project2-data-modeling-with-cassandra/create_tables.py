import cassandra
from cassandra.cluster import Cluster
from cql_queries import create_table_queries, drop_table_queries


def create_database_connection():
    """
    - Creates and connects to the sparkifydb
    - Returns the session and cluster to sparkifydb
    """
    # This should make a connection to a Cassandra instance your local machine
    # (127.0.0.1)
    cluster = Cluster(['127.0.0.1'])

    # To establish connection and begin executing queries, need a session
    session = cluster.connect()

    # TO-DO: Create a Keyspace
    try:
        session.execute("""
        CREATE KEYSPACE IF NOT EXISTS sparkifydb 
        WITH REPLICATION = 
        { 'class' : 'SimpleStrategy', 'replication_factor' : 1 }"""
                        )
    # TO-DO: Set KEYSPACE to the keyspace specified above
        session.set_keyspace('sparkifydb')
        print('Initial create cassandra connection')
    except Exception as e:
        print(e)
    return session, cluster


def drop_tables(session):
    """
    Drops each table using the queries in `drop_table_queries` list.
    """
    for query in drop_table_queries:
        session.execute(query)
        print('Clean existing table: ' + query)


def create_tables(session):
    """
    Creates each table using the queries in `create_table_queries` list. 
    """
    for query in create_table_queries:
        session.execute(query)
        print('Created table: ' + query)


def main():
    """
    - Drops (if exists) and Creates the sparkify database. 

    - Establishes connection with the sparkify database and gets
    session to it.  

    - Create a Keyspace and Set KEYSPACE to the keyspace

    - Drops all the tables if exists.  

    - Creates all tables needed. 

    - Finally, closes the connection. 
    """
    print('---Running create_tables.py---')
    session, cluster = create_database_connection()

    drop_tables(session)
    create_tables(session)

    session.shutdown()
    cluster.shutdown()
    print('Close cassandra session/cluster connection')


if __name__ == "__main__":
    main()
