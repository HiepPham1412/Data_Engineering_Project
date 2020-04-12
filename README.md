#  Purpose of the project

LendingClub is a peer to peer lending platform which provides credit to borrowers who could not or dont want to get credit from commercial banks.
This project aims to build a simple ETL process to transform LendingClub raw data in csv format stored in S3 to proper database in Redshift. 
This helps to monitor the performance of the loans. In addition, these data tables are important for credit modelling as well.

# Raw data
`lending club loan` data is stored in a S3 bucket: s3://lending-club-project/raw_data. This data in csv format and contain all information of more than 2 millions loan. 
The originial data is published by LendingClub which can be found at https://www.kaggle.com/wendykan/lending-club-loan-data

`state_demo` data is retreived from `https://public.opendatasoft.com/explore/dataset/us-cities-demographics/download/?format=json&timezone=Europe/Berlin&lang=en`


# Data base schema

The database contains following tables:

**loans related tables**:

`loans` table contains essential information of a loan at origination time such as loan amount, loan term, ...

`borrowers` table contains borrower's information (employment length, state, ...)

`credit_history` table stores borrowers credit history  (number of credit lines, revolving credit balance,...)

`payment` table shows the most updated the payment status of each loan (loan status, principal payment amount, interest payment amount)

`bad_debt_settlement` table records the debt collection status of defaulted loans

`hardship` table keeps all information of loans which applies for a hardship payment plan


**State demographic**

`state_demo` table shows the basic demographic information of each states. This is helpful for credit modelling purpose.
 
 **Data base diagram**

![alt text](/img/data_base_dig.png "Data base diagram")

# ETL flow

Raw data from S3, API >> `spark ETL` >> S3 >> `redshift_etl` >> Redshift database

# Scenario analysis 
- If the data was increased by 100x: as the data transfomation steps is done in Spark, so it would be easy to use a more powerful EMR cluster
- If the pipelines would be run on a daily basis by 7 am every day: need to build a scheduler to automatically execute the script. Airflow is a possible solution to handle this.
- If the database needed to be accessed by 100+ people: it would be easy to switch to a more powerful Redshift cluster

# How to?

**Scripts**:

`Data_Quality_Check.ipynb`: check data quality and address these issues during the ETL process

`spark_etl.py`: script to load data from S3 bucket, and API from a website, transform the data and write back to a S3 bucket. 
Please note that this script only runs on an EMR cluster (us-west2).

`create_tables.py`: script to create data tables in Redshift cluster 

`redshift_etl.py`: script to do ETL process from S3 to Redshift database. Please note that this cluster should locates in us-west-2 region as S3 bucket is in us-west-2 region.

`sql_queries.py`: script storing sql queries used in `create_tables.py` and `etl.py`  

`config.cfg`: config file to store some meta data  

`Test ETL implementation.ipynb` notebooks to test the whole ETL process  

**Execution steps**:

- Run `Data_Quality_Check.ipynb` to have an idea how the data looks like
- Run `spark_etl.py` to transform the data and write to another S3 bucket 
- Run `redshift_etl.py`
- RUn `Test ETL implementation.ipynb` notebook to check the data quality as well as the whole ETL process




