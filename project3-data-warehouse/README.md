# Project 3: DATA WAREHOUSE
In this project, you'll apply what you've learned on data warehouses and AWS to build an ETL pipeline for a database hosted on Redshift. To complete the project, you will need to load data from S3 to staging tables on Redshift and execute SQL statements that create the analytics tables from these staging tables.

# Explanation of the files in the repository
1. ```./redshift_helper/create_cluster.py```: base on value from dwh.cfg to create aws redshift cluster
2. ```redshift_helper/delete_cluster.py```: base on value from dwh.cfg to delete aws redshift cluster
3. ```create_tables.py```: drops and creates our tables. We run this file to reset our tables before each time we run our ETL scripts.
4. ```sql_queries_dist.py```: contains all our sql queries, and is imported into the last three files above.
5. ```sql_queries_nodist.py```: contains all our sql queries, and is imported into the last three files above.
6. ```etl.py```: reads and processes files from song_data and log_data and loads them into our tables. We can fill this out based on our work in the ETL notebook.
7. ```analytic.ipynb```: To analyze/compare performance between using dist key and no dist key
8. ```README.md```: provides discussion on our project.
9. ```dwh.cfg```: to store all configurations of this project

# Environment set up (In case run locally)
1. Set up python, postgresSQL
2. Install psycopg2: ```pip install psycopg2```
3. Install pandas: ```pip install pandas```
4. Install ipython-sql to run test.ipynb locally: ```pip install ipython-sql```
5. install boto3: ```pip install boto3```
6. install matplotlib: ```pip install matplotlib```
7. For jupyter notebook: change kernel to use python version above 3.

# Run scripts

1. Create/use IAM user with attach policy ```AmazonRedshiftFullAccess```, ```AmazonS3ReadOnlyAccess```
2. Manually input AWS  KEY and AWS Secret KEY into ```dwh.cfg``` 
3. Run python file ```redshift_helper/create_cluster.py```
4. ```python create_table.py```
5. ```python etl.py```
6. Run ```analytic.ipynb```
7. Run python file ```redshift_helper/delete_cluster.py```

