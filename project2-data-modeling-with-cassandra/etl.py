import os
import csv
import glob
from cql_queries import *
from create_tables import create_database_connection


def collect_event_datafile_to_csv():
    """
    This procedure processes a csv file whose filepath has been provided from cql_queries.py.
    Then create a smaller event data csv file called event_datafile_full csv
    """
    file_path_list = []
    # checking your current working directory
    print('working directory: ' + os.getcwd())

    # Get your current folder and subfolder event data
    filepath = os.getcwd() + event_folder

    # checking your folder and subfolder event data directory
    print('folder and subfolder event data: ' + filepath)

    # Create a for loop to create a list of files and collect each filepath
    for root, dirs, files in os.walk(filepath):
    
    # join the file path and roots with the subdirectories using glob
        file_path_list = glob.glob(os.path.join(root,'*'))
        print(file_path_list)

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
                
    # uncomment the code below if you would like to get total number of rows 
    # print(len(full_data_rows_list))
    # uncomment the code below if you would like to check to see what the list of event data rows will look like
    # print(full_data_rows_list)

    # creating a smaller event data csv file called event_datafile_full csv that will be used to insert data into the \
    # Apache Cassandra tables
    csv.register_dialect('myDialect', quoting=csv.QUOTE_ALL, skipinitialspace=True)

    # check if whether event_datafile_new.csv is existing or not then remove
    if os.path.exists(csv_file):
        os.remove(csv_file)
        print('Removed file: ' + csv_file)
        
    with open(csv_file, 'w', encoding = 'utf8', newline='') as f:
        writer = csv.writer(f, dialect='myDialect')
        writer.writerow(['artist','firstName','gender','itemInSession','lastName','length',\
                    'level','location','sessionId','song','userId'])
        for row in full_data_rows_list:
            if (row[0] == ''):
                continue
            writer.writerow((row[0], row[2], row[3], row[4], row[5], row[6], row[7], row[8], row[12], row[13], row[16]))


def process_csv_file(session):
    """
    This procedure processes a csv file whose filepath has been provided from cql_queries.py.
    Then insert handled records into the song_table, artist_table, user_table.

    INPUTS: 
    * session the session variable (cassandra connection)
    """
    # check the number of rows in your csv file
    with open(csv_file, 'r', encoding = 'utf8') as f:
        num_rows = sum(1 for line in f)
    with open(csv_file, encoding = 'utf8') as f:
        csvreader = csv.reader(f)
        next(csvreader) # skip header
        for i, line in enumerate(csvreader, 2):
    ## TO-DO: Assign the INSERT statements into the `query` variable
            ## TO-DO: Assign which column element should be assigned for each column in the INSERT statement.
            ## For e.g., to INSERT artist_name and user first_name, you would change the code below to `line[0], line[1]`
            session.execute( song_table_insert, (int(line[8]), int(line[3]), line[0], line[9], float(line[5])))  #Refer: https://knowledge.udacity.com/questions/529901
            print('{}/{} rows processed into song_table.'.format(i, num_rows))
            session.execute(artist_table_insert, (int(line[10]), int(line[8]), line[0], line[9], int(line[3]), line[1], line[4]))
            print('{}/{} rows processed into artist_table.'.format(i, num_rows))
            session.execute(user_table_insert, (line[9], int(line[10]), line[1], line[4]))
            print('{}/{} rows processed into user_table.'.format(i, num_rows)) 


def main():
    # Create cassandra connection
    session, cluster = create_database_connection()

    # Create new csv to collect data from event file
    collect_event_datafile_to_csv()

    # insert record
    process_csv_file(session)

    session.shutdown()
    cluster.shutdown()


if __name__ == "__main__":
    main()