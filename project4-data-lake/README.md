# Project 4: Data Lake
In this project, you'll apply what you've learned on Spark and data lakes to build an ETL pipeline for a data lake hosted on S3. To complete the project, you will need to load data from S3, process the data into analytics tables using Spark, and load them back into S3. You'll deploy this Spark process on a cluster using AWS.

# Schema for Song Play Analysis


## Fact tables
### songplay tables
Primary key: songplay_id
Foreign key: start_time, user_id, song_id, artist_id
| Column      | Type        | Nullable | Explanation                                                                                   |
| ----------- | ----------- | -------- | --------------------------------------------------------------------------------------------- |
| songplay_id | SERIAL      |          | In Log dataset, there are no songplay_id => songplay_id should auto increment (SERIAL)        |
| start_time  | TIMESTAMP   | not null | "registration" log dataset                                                                    |
| user_id     | INT         | not null | "userId" log datase                                                                           |
| level       | VARCHAR(4)  | not null | "level" log dataset                                                                           |
| song_id     | VARCHAR(18) | not null | "song"   log dataset =  "title" song dataset => query song_id (limit 18 characters)           |
| artist_id   | VARCHAR(18) | not null | "artist"  log dataset =  "artist_name" song data set => query artist_id (limit 18 characters) |
| session_id  | INT         | not null | "sessionId" log dataset                                                                       |
| location    | VARCHAR     | not null | "location" log dataset                                                                        |
| user_agent  | VARCHAR     | not null | "userAgent" log dataset                                                                       |

## Dimension tables

### users tables
Primary key: user_id
| Column     | Type       | Nullable | Explanation             |
| ---------- | ---------- | -------- | ----------------------- |
| user_id    | INT        | not null | "userId" log datase     |
| first_name | VARCHAR    | not null | "firstName" log dataset |
| last_name  | VARCHAR    | not null | "lastName" log dataset  |
| gender     | CHAR(1)    | not null | "gender" log dataset    |
| level      | VARCHAR(4) | not null | "level" log dataset     |

### songs tables
Primary key: song_id
| Column    | Type        | Nullable | Explanation                                  |
| --------- | ----------- | -------- | -------------------------------------------- |
| song_id   | VARCHAR(18) | not null | "song_id" song dataset (limit 18 characters) |
| title     | VARCHAR     | not null | "title" song dataset                         |
| artist_id | VARCHAR(18) | not null | "artist_id" song dataset                     |
| year      | INT         | not null | "year" song dataset                          |
| duration  | FLOAT       | not null | "duration" song dataset                      |

### artists tables
Primary key: artist_id
| Column     | Type        | Nullable | Explanation                                    |
| ---------- | ----------- | -------- | ---------------------------------------------- |
| artist_id  | VARCHAR(18) | not null | "artist_id" song dataset (limit 18 characters) |
| name       | VARCHAR     | not null | "artist_name" song dataset                     |
| location   | VARCHAR     |          | "artist_location" song dataset                 |
| latitude   | FLOAT       |          | "artist_latitude" song dataset                 |
| longtitude | FLOAT       |          | "artist_longtitute" song dataset               |

### time tables
Primary key: start_time
| Column     | Type      | Nullable | Explanation                                                  |
| ---------- | --------- | -------- | ------------------------------------------------------------ |
| start_time | TIMESTAMP | not null | "ts" log dataset                                             |
| hour       | INT       | not null | break down from start_time                                   |
| day        | INT       |          | break down from start_time                                   |
| week       | INT       |          | break down from start_time                                   |
| month      | INT       |          | break down from start_time                                   |
| year       | INT       |          | break down from start_time                                   |
| weekday    | INT       |          | break down from start_time (0 Monday, 1 Tuesday, … 6 Sunday) |

# Explanation of the files in the repository
1. ```./s3_helper/create_cluster.py```: base on value from dl.cfg to create s3 bucket
2. ```./s3_helper/delete_cluster.py```: base on value from dl.cfg to delete s3 bucket
3. ```etl.ipynb```: reads and processes a single file from song_data and log_data and loads the data into our tables. This notebook contains detailed instructions on the ETL process for each of the tables.
4. ```etl.py```: reads and processes files from song_data and log_data and loads them into s3 output bucket
5. ```query.ipynb```: To check the result
6. ```README.md```: provides discussion on our project.

# Environment set up (In case run locally)
1. Install pandas: ```pip install pandas```
2. Install pyspark: ```pip install pyspark```

# Run scripts

1. Create/use IAM user with attach policy ```AmazonS3FullAccess```
2. Manually input AWS  KEY and AWS Secret KEY into ```dwh.cfg``` 
3. ```python s3_helper/create_s3.py```
4. ```python etl.py``` wait for loading (around 1 hour)
5. run  ```query.ipynb``` file to check the result
6. ```python s3_helper/delete_s3.py```

