## Week 3 Homework - Exequiel Santucho

To perform this task, the following resources have been followed:

Data Engineering Zoomcamp - 03-workflow-orchestration notes: [Link](https://github.com/DataTalksClub/data-engineering-zoomcamp/tree/main/03-data-warehouse)

From above, consulted videos are listed:

- [Data Warehouse and BigQuery](https://youtu.be/jrHljAoD6nM&list=PL3MmuxUbc_hJed7dXYoJw8DoCuVHhGEQb&index=34)

- [Partioning and Clustering](https://www.youtube.com/watch?v=-CqXf7vhhDs&list=PL3MmuxUbc_hJed7dXYoJw8DoCuVHhGEQb&index=36)

- [Best Practices](https://www.youtube.com/watch?v=k81mLJVX08w&list=PL3MmuxUbc_hJed7dXYoJw8DoCuVHhGEQb&index=37)

- [Internals of BigQuery](https://youtu.be/eduHi1inM4s&list=PL3MmuxUbc_hJed7dXYoJw8DoCuVHhGEQb&index=37)

Advanced topics:

- [Machine Learning in Big Query](https://youtu.be/B-WtpB0PuG4&list=PL3MmuxUbc_hJed7dXYoJw8DoCuVHhGEQb&index=34)

- [Deploying Machine Learning model from BigQuery](https://youtu.be/BjARzEWaznU&list=PL3MmuxUbc_hJed7dXYoJw8DoCuVHhGEQb&index=39)

From above, consulted specific codes and instructions:

- [Big Query basic SQL](https://github.com/DataTalksClub/data-engineering-zoomcamp/blob/main/03-data-warehouse/big_query.sql)

- [SQL for ML in BigQuery](https://github.com/DataTalksClub/data-engineering-zoomcamp/blob/main/03-data-warehouse/big_query_ml.sql)

- [BigQuery ML Tutorials](https://cloud.google.com/bigquery-ml/docs/tutorials)

- [BigQuery ML Reference Parameter](https://cloud.google.com/bigquery-ml/docs/analytics-reference-patterns)

- [Hyper Parameter tuning](https://cloud.google.com/bigquery-ml/docs/reference/standard-sql/bigqueryml-syntax-create-glm)

- [Feature preprocessing](https://cloud.google.com/bigquery-ml/docs/reference/standard-sql/bigqueryml-syntax-preprocess-overview)

- [Steps to extract and deploy model with docker](https://github.com/DataTalksClub/data-engineering-zoomcamp/blob/main/03-data-warehouse/extract_model.md)

---

ATTENTION: At the end of the submission form, you will be required to include a link to your GitHub repository or other public code-hosting site. This repository should contain your code for solving the homework. If your solution includes code that is not in file format (such as SQL queries or shell commands), please include these directly in the README file of your repository.

**<u>Important Note</u>**: For this homework we will be using the 2022 Green Taxi Trip Record Parquet Files from the New York City Taxi Data found here:  https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page 
If you are using orchestration such as Mage, Airflow or Prefect do not load the data into Big Query using the orchestrator. Stop with loading the files into a bucket. 
<u>NOTE:</u> 

You will need to use the PARQUET option files when creating an External Table

<b>SETUP:</b>
Create an external table using the Green Taxi Trip Records Data for 2022.
Create a table in BQ using the Green Taxi Trip Records for 2022 (do not partition or cluster this table).

</p>

---

**SOLUTIONS**

Note: To create the external tables it was decided to manually download the files from the aforementioned website. These files will then be manually uploaded to a bucket called `module3-ny-green-taxi-data`, from where the external table is then created in BigQuery. In [this repository](https://github.com/exequiel-santucho/data-engineering-zoomcamp/tree/main/03-data-warehouse/extras)  a quick-hack way to do this by code is offered. They could also have been uploaded via Mage.



#### Creating External Table and [Materialized, Normal, Regular] Table in BigQuery

In the BigQuery Service, we need to create a Dataset called `ny_taxi_data` inside the Project ID `zeta-serenity-41242`. After that we can create the External Ttable `external_green_tripdata` running the following query:

```sql
-- Creating EXTERNAL TABLE referring to gcs path
CREATE OR REPLACE EXTERNAL TABLE `zeta-serenity-412422.ny_taxi_data.external_green_tripdata`
OPTIONS (
  format = 'parquet',
  uris = ['gs://module3-ny-green-taxi-data/green_tripdata_2022-*.parquet']
);
```

We note that the bucket references to `module3-ny-green-taxi-data` in Google Cloud Storage Service (Reference: [Create Cloud Storage external tables)](https://cloud.google.com/bigquery/docs/external-data-cloud-storage). After that we create the [Materialized, Normal, Regular] Table or simply Table, running the following:

```sql
-- Creating TABLE referring to local above created table
CREATE OR REPLACE TABLE zeta-serenity-412422.ny_taxi_data.green_tripdata AS
SELECT * FROM zeta-serenity-412422.ny_taxi_data.external_green_tripdata
;
```

## Question 1:

Question 1: What is count of records for the 2022 Green Taxi Data??

- 65,623,481
- 840,402
- 1,936,423
- 253,647

**Answer**: 840,402

<u>Code</u>:

```sql
SELECT count(1) AS counts
FROM `zeta-serenity-412422.ny_taxi_data.external_green_tripdata`;
```

## Question 2:

Write a query to count the distinct number of PULocationIDs for the entire dataset on both the tables.</br> 
What is the estimated amount of data that will be read when this query is executed on the External Table and the Table?

- 0 MB for the External Table and 6.41MB for the Materialized Table
- 18.82 MB for the External Table and 47.60 MB for the Materialized Table
- 0 MB for the External Table and 0MB for the Materialized Table
- 2.14 MB for the External Table and 0MB for the Materialized Table

**Answer**: *0 MB for the External Table and 6.41MB for the Materialized Table*

In "JOB INFORMATION" we can see that the estimated amount of data that will be read is: for Table 6.41 MB (10 MB billed) and for External Table 0 B (0 B billed)

<u>Code</u>:

```sql
SELECT COUNT(DISTINCT PULocationID) AS count_pulocationsid_distinct
FROM zeta-serenity-412422.ny_taxi_data.green_tripdata
;
```

```sql
 SELECT COUNT(DISTINCT PULocationID) AS count_pulocationsid_distinct
FROM zeta-serenity-412422.ny_taxi_data.external_green_tripdata
;
```

## Question 3:

How many records have a fare_amount of 0?

- 12,488
- 128,219
- 112
- 1,622

**Answer**: 1622

<u>Code</u>:

```sql
SELECT COUNT(*) AS zero_values
FROM zeta-serenity-412422.ny_taxi_data.external_green_tripdata
WHERE fare_amount=0;
```

## Question 4:

What is the best strategy to make an optimized table in Big Query if your query will always order the results by PUlocationID and filter based on lpep_pickup_datetime? (Create a new table with this strategy)

- Cluster on lpep_pickup_datetime Partition by PUlocationID
- Partition by lpep_pickup_datetime  Cluster on PUlocationID
- Partition by lpep_pickup_datetime and Partition by PUlocationID
- Cluster on by lpep_pickup_datetime and Cluster on PUlocationID

**Answer**: *Partition by lpep_pickup_datetime Cluster on PUlocationID*

This assumes that the most of queries are based upon dates and use it as a filter, and then ordered by PUlocationID.

<u>Code</u>:

```sql
CREATE OR REPLACE TABLE zeta-serenity-412422.ny_taxi_data.green_tripdata_partitoned_clustered
PARTITION BY DATE(lpep_pickup_datetime)
CLUSTER BY PUlocationID AS
SELECT * FROM zeta-serenity-412422.ny_taxi_data.external_green_tripdata;

-- Bytes processed 114.11 MB (115 MB Bytes billed)
```



## Question 5:

Write a query to retrieve the distinct PULocationID between lpep_pickup_datetime
06/01/2022 and 06/30/2022 (inclusive)

Use the materialized table you created earlier in your from clause and note the estimated bytes. Now change the table in the from clause to the partitioned table you created for question 4 and note the estimated bytes processed. What are these values? 

Choose the answer which most closely matches.

- 22.82 MB for non-partitioned table and 647.87 MB for the partitioned table
- 12.82 MB for non-partitioned table and 1.12 MB for the partitioned table
- 5.63 MB for non-partitioned table and 0 MB for the partitioned table
- 10.31 MB for non-partitioned table and 10.31 MB for the partitioned table

**Answer**: *12.82 MB for non-partitioned table and 1.12 MB for the partitioned table*

<u>Code</u>:

```sql
SELECT DISTINCT PULocationID as distinct_pulocationid
FROM zeta-serenity-412422.ny_taxi_data.green_tripdata
WHERE DATE(lpep_pickup_datetime) BETWEEN '2022-06-01' AND '2022-06-30'
;
-- Bytes processed 12.82 MB (13 MB Bytes billed)
```

```sql
 SELECT DISTINCT PULocationID as distinct_pulocationid
FROM zeta-serenity-412422.ny_taxi_data.green_tripdata_partitoned_clustered
WHERE DATE(lpep_pickup_datetime) BETWEEN '2022-06-01' AND '2022-06-30'
;
-- Bytes processed 1.12 MB (10 MB Bytes billed)
```

## Question 6:

Where is the data stored in the External Table you created?

- Big Query
- GCP Bucket
- Big Table
- Container Registry

**Answer**: *GCP Bucket*

Checking the docs at https://cloud.google.com/bigquery/docs/external-data-sources: External tables are similar to standard BigQuery tables, in that these tables store their metadata and schema in BigQuery storage. However, their data resides in an external source.

## Question 7:

It is best practice in Big Query to always cluster your data:

- True
- False

**Answer**: *True*

## (Bonus: Not worth points) Question 8:

No Points: Write a `SELECT count(*)` query FROM the materialized table you created. How many bytes does it estimate will be read? Why?

**Answer**: This query will process 0 B when run. I think this is that way because storage info (count(*) = number of rows) was calculated when created the table.

<u>Code</u>:

```sql
SELECT count(*) FROM zeta-serenity-412422.ny_taxi_data.green_tripdata;
```

## Submitting the solutions

* Form for submitting: https://courses.datatalks.club/de-zoomcamp-2024/homework/hw3
