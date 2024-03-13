## Module 4 Homework - Exequiel Santucho

To perform this task, the following resources have been followed:

- Data ingestion with dlt notes: [Link](https://github.com/exequiel-santucho/data-engineering-zoomcamp/tree/main/04-analytics-engineering)

From above, consulted videos are listed:

- [Analytics Engineering Basics](https://www.youtube.com/watch?v=uF76d5EmdtU&list=PL3MmuxUbc_hJed7dXYoJw8DoCuVHhGEQb&index=41)

- [What is dbt?](https://www.youtube.com/watch?v=gsKuETFJr54&list=PLaNLNpjZpzwgneiI-Gl8df8GCsPYp_6Bs&index=6)

- [Start Your dbt Project BigQuery and dbt Cloud (Alternative A)](https://www.youtube.com/watch?v=J0XCDyKiU64&list=PLaNLNpjZpzwgneiI-Gl8df8GCsPYp_6Bs&index=5)

- [Build the First dbt Models](https://www.youtube.com/watch?v=ueVy2N54lyc&list=PLaNLNpjZpzwgneiI-Gl8df8GCsPYp_6Bs&index=4)

- [Testing and Documenting the Project](https://www.youtube.com/watch?v=2dNJXHFCHaY&list=PLaNLNpjZpzwgneiI-Gl8df8GCsPYp_6Bs&index=3)

- [Deployment Using dbt Cloud (Alternative A)](https://www.youtube.com/watch?v=V2m5C0n8Gro&list=PLaNLNpjZpzwgneiI-Gl8df8GCsPYp_6Bs&index=7)

- [Visualising the data with Google Data Studio (Alternative A)](https://www.youtube.com/watch?v=39nLTs74A3E&list=PL3MmuxUbc_hJed7dXYoJw8DoCuVHhGEQb&index=49)

---



In this homework, we'll use the models developed during the week 4 videos and enhance the already presented dbt project using the already loaded Taxi data for fhv vehicles for year 2019 in our DWH.

This means that in this homework we use the following data [Datasets list](https://github.com/DataTalksClub/nyc-tlc-data/)

* Yellow taxi data - Years 2019 and 2020
* Green taxi data - Years 2019 and 2020 
* fhv data - Year 2019. 

We will use the data loaded for:

* Building a source table: `stg_fhv_tripdata`
* Building a fact table: `fact_fhv_trips`
* Create a dashboard 

If you don't have access to GCP, you can do this locally using the ingested data from your Postgres database
instead. If you have access to GCP, you don't need to do it for local Postgres - only if you want to.

> **Note**: if your answer doesn't match exactly, select the closest option 

Note:

If necesariy, you can configure gcloud credentials before starging, with:

```
gcloud config set project <your-project-id>
```

### Question 1:

**What happens when we execute dbt build --vars '{'is_test_run':'true'}'**
You'll need to have completed the ["Build the first dbt models"](https://www.youtube.com/watch?v=UVI30Vxzd6c) video. 

- It's the same as running *dbt build*
- It applies a _limit 100_ to all of our models
- It applies a _limit 100_ only to our staging models
- Nothing

**Answer**:  It applies a *limit 100* only to our staging models

### Question 2:

**What is the code that our CI job will run? Where is this code coming from?**  

- The code that has been merged into the main branch
- The code that is behind the creation object on the dbt_cloud_pr_ schema
- The code from any development branch that has been opened based on main
- The code from the development branch we are requesting to merge to main

**Answer**: The code from the development branch we are requesting to merge to main

### Question 3 (2 points)

**What is the count of records in the model fact_fhv_trips after running all dependencies with the test run variable disabled (:false)?**  
Create a staging model for the fhv data, similar to the ones made for yellow and green data. Add an additional filter for keeping only records with pickup time in year 2019.
Do not add a deduplication step. Run this models without limits (is_test_run: false).

Create a core model similar to fact trips, but selecting from stg_fhv_tripdata and joining with dim_zones.
Similar to what we've done in fact_trips, keep only records with known pickup and dropoff locations entries for pickup and dropoff locations. 
Run the dbt model without limits (is_test_run: false).

- 12998722
- 22998722
- 32998722
- 42998722

**Answer**: 22998722

```sql
select count(*) from {{ ref('fact_fhv_trips') }}
```

### Question 4 (2 points)

**What is the service that had the most rides during the month of July 2019 month with the biggest amount of rides after building a tile for the fact_fhv_trips table?**

Create a dashboard with some tiles that you find interesting to explore the data. One tile should show the amount of trips per month, as done in the videos for fact_trips, including the fact_fhv_trips data.

- FHV
- Green
- Yellow
- FHV and Green

**Answer**: Yellow

```sql
with green_yellow as (
    select service_type, count(*) as total_records
    from {{ ref('fact_trips') }}
    -- where extract(year from pickup_datetime) = 2019
    -- and extract(month from pickup_datetime) = 7
    group by 1
),
fhv as (
    select service_type, count(*) as total_records
    from {{ ref('fact_fhv_trips') }}
    -- where extract(year from pickup_datetime) = 2019
    -- and extract(month from pickup_datetime) = 7
    group by 1
)

select * from green_yellow
union all
select * from fhv
-- Answer: Fhv. But the real answer is Yellow. I have an error in constructing green_yellow table. The results are not consistent with the solution. If I filter the
-- data by datetime (july 2019) the green_yellow table is empty. It's a matter of availability of this data, but I think the code lines are correct.
```

## Submitting the solutions

* Form for submitting: https://courses.datatalks.club/de-zoomcamp-2024/homework/hw4

Deadline: 22 February (Thursday), 22:00 CET

## Solution (To be published after deadline)

* Video: 
* Answers:
  * Question 1: 
  * Question 2: 
  * Question 3: 
  * Question 4: 
