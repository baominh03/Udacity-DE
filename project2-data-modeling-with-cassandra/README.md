# Project 2: Data Modeling on Apache Cassandra
n this project, you'll apply what you've learned on data modeling with Apache Cassandra and complete an ETL pipeline using Python. To complete the project, you will need to model your data by creating tables in Apache Cassandra to run queries. You are provided with part of the ETL pipeline that transfers data from a set of CSV files within a directory to create a streamlined CSV file to model and insert data into Apache Cassandra tables.

We have provided you with a project template that takes care of all the imports and provides a structure for ETL pipeline you'd need to process this data.

# Schema for Song Play Analysis

## 1. Give me the artist, song title and song's length in the music app history that was heard 
### song_table
| Column          | Type  | Primary Key | Cluster Key | Explaination                              |
| --------------- | ----- | ----------- | ----------- | ----------------------------------------- |
| session_id      | INT   | Y           |             | where sessionId first => session_id is PK |
| item_in_session | INT   |             | Y           | item_in_session is CK                     |
| artist          | TEXT  |             |             |                                           |
| song_title      | TEXT  |             |             |                                           |
| length          | FLOAT |             |             |                                           |

## 2. Give me only the following: name of artist, song (sorted by itemInSession) and user (first and last name) for userid = 10, sessionid = 182
### artist_table
| Column          | Type | Primary Key | Cluster Key | Explaination                                     |
| --------------- | ---- | ----------- | ----------- | ------------------------------------------------ |
| user_id         | INT  | Y           |             | where userId first => user_id is PK              |
| session_id      | INT  |             | Y           | session_id is CK                                 |
| artist          | TEXT |             |             |                                                  |
| song_title      | TEXT |             |             |                                                  |
| item_in_session | INT  |             | Y           | sorted by itemInSession => item_in_session is CK |
| first_name      | TEXT |             |             |                                                  |
| last_name       | TEXT |             |             |                                                  |

## 3. Give me every user name (first and last) in my music app history who listened to the song 'All Hands Against His Own'
### user_table
| Column     | Type | Primary Key | Cluster Key | Explaination                         |
| ---------- | ---- | ----------- | ----------- | ------------------------------------ |
| song_title | TEXT | Y           |             | where song first => song_title is PK |
| user_id    | INT  |             | Y           | user_id is CK                        |
| first_name | TEXT |             |             |                                      |
| last_name  | TEXT |             |             |                                      |


# Explanation of the files in the repository
1. ```create_tables.py```: drops and creates our tables. We run this file to reset our tables before each time we run our ETL scripts.
2. ```cql_queries.py```: contains all our cql queries, and is imported into the last three files above.
3. ```etl.ipynb```: reads and processes event csv files and loads the data into our tables. This notebook contains detailed instructions on the ETL process for each of the tables.
4. ```etl.py```: reads and processes event csv files and loads them into our tables. We can fill this out based on our work in the ETL notebook.
5. ```test_cql.ipynb```: let us check our database with three queries.
6. ```README.md```: provides discussion on our project.

# Environment set up (In case run locally - window only)
1. Install python version 2.7  (compatible with cassandra 3.11.xx)
2. Install jdk version 8u211 (compatible with cassandra 3.11.xx)
3. Install python cassandra ```pip install cassandra-driver```
4. Run cassandra server before running ```cassandra```

# Run scripts on

1. Config #HANDLE CSV path in ```test_cql.py``` to run on locally or udacity workspace
2. Run ```cassandra``` (locally - window only. skip this step if you are running on udacity workspace)
3.  ```python create_tables.py```
4. ```python etl.py``` or run manually ```etl.ipynb``` file
5. Run ```test_cql.ipynb``` to check up result

