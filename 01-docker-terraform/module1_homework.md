## Module 1 Homework - Exequiel Santucho

To perform this task, the following resources have been followed:

- Data Engineering Zoomcamp - 01-docker-terraform notes: [Link](https://github.com/DataTalksClub/data-engineering-zoomcamp/tree/main/01-docker-terraform)

- Community notes from Alvaro Navas: [Link](https://github.com/ziritrion/dataeng-zoomcamp/blob/main/notes/1_intro.md)

- From above, consulted videos are listed:
  
  - [Introduction]([Data Engineering Zoomcamp 2023 - YouTube](https://www.youtube.com/watch?v=-zpVha7bw5A))
  
  - [Introduction to Docker]([DE Zoomcamp 1.2.1 - Introduction to Docker - YouTube](https://www.youtube.com/watch?v=EYNwNlOrpr0&list=PL3MmuxUbc_hJed7dXYoJw8DoCuVHhGEQb))
  
  - [Ingesting NY Taxi Data to Postgres]([DE Zoomcamp 1.2.2 - Ingesting NY Taxi Data to Postgres - YouTube](https://www.youtube.com/watch?v=2JM-ziJt0WI&list=PL3MmuxUbc_hJed7dXYoJw8DoCuVHhGEQb))
  
  - [Connecting pgAdmin and Postgres]([DE Zoomcamp 1.2.3 - Connecting pgAdmin and Postgres - YouTube](https://www.youtube.com/watch?v=hCAIVe9N0ow&list=PL3MmuxUbc_hJed7dXYoJw8DoCuVHhGEQb))
  
  - [Putting the ingestion script into Docker]([DE Zoomcamp 1.2.4 - Dockerizing the Ingestion Script - YouTube](https://www.youtube.com/watch?v=B1WwATwf-vY&list=PL3MmuxUbc_hJed7dXYoJw8DoCuVHhGEQb))
  
  - [Running Postgres and pgAdmin with Docker-Compose]([DE Zoomcamp 1.2.5 - Running Postgres and pgAdmin with Docker-Compose - YouTube](https://www.youtube.com/watch?v=hKI6PkPhpa0&list=PL3MmuxUbc_hJed7dXYoJw8DoCuVHhGEQb))
  
  - [SQL refresher]([DE Zoomcamp 1.2.6 - SQL Refreshser - YouTube](https://www.youtube.com/watch?v=QEcps_iskgg&list=PL3MmuxUbc_hJed7dXYoJw8DoCuVHhGEQb))
  
  - [Introduction to GCP (Google Cloud Platform)]([DE Zoomcamp 1.1.1 - Introduction to Google Cloud Platform - YouTube](https://www.youtube.com/watch?v=18jIzE41fJ4&list=PL3MmuxUbc_hJed7dXYoJw8DoCuVHhGEQb))
  
  - [Introduction Terraform: Concepts and Overview]([DE Zoomcamp 1.3.1 - Terraform Primer - YouTube](https://youtu.be/s2bOYDCKl_M))
  
  - [Terraform Basics: Simple one file Terraform Deployment]([DE Zoomcamp 1.3.2 - Terraform Basics - YouTube](https://youtu.be/Y2ux7gq3Z0o))
  
  - [Deployment with a Variables File]([DE Zoomcamp 1.3.3 - Terraform Variables - YouTube](https://youtu.be/PBi0hHjLftk))
  
  - [Introduction to Terraform Concepts & GCP Pre-Requisites]([DE Zoomcamp 1.3.1 - Introduction to Terraform Concepts &amp; GCP Pre-Requisites - YouTube](https://www.youtube.com/watch?v=Hajwnmj0xfQ&list=PL3MmuxUbc_hJed7dXYoJw8DoCuVHhGEQb&index=7))
  
  - [Creating GCP Infrastructure with Terraform]([DE Zoomcamp 1.3.2 - Creating GCP Infrastructure with Terraform - YouTube](https://www.youtube.com/watch?v=dNkEgO-CExg&list=PL3MmuxUbc_hJed7dXYoJw8DoCuVHhGEQb&index=11))

- From above, consulted specific codes and instructions:
  
  - [Docker+Postgres](https://github.com/DataTalksClub/data-engineering-zoomcamp/tree/main/01-docker-terraform/2_docker_sql)
  
  - [Terraform](https://github.com/DataTalksClub/data-engineering-zoomcamp/tree/main/01-docker-terraform/1_terraform_gcp)
  
  - [Configuring Terraform and GCP SDK on Windows](https://github.com/DataTalksClub/data-engineering-zoomcamp/blob/main/01-docker-terraform/1_terraform_gcp/windows.md)
    
    ## 

## Docker & SQL

In this homework we'll prepare the environment 
and practice with Docker and SQL

## Question 1. Knowing docker tags

Run the command to get information on Docker 

```docker --help```

**Answer**: Done. I get a list of possible dockers's commands.

Now run the command to get help on the "docker build" command:

```docker build --help```

**Answer**: Done. I get a list of options referred to *build* command.

Do the same for "docker run".

**Answer**: Done. I get a list of options referred to *run* command.

Which tag has the following text? - *Automatically remove the container when it exits* 

- `--delete`
- `--rc`
- `--rmc`
- `--rm`

**Answer**:  That text is referred to `--rm` tag.

## Question 2. Understanding docker first run

Run docker with the python:3.9 image in an interactive mode and the entrypoint of bash.
Now check the python modules that are installed ( use ```pip list``` ). 

What is version of the package *wheel* ?

- 0.42.0
- 1.0.0
- 23.0.1
- 58.1.0

**Answer**: The version is ``0.42.0``. Commands used in MINGW64 console to get version of mentioned package:

```
(bash prompt)   docker run -it python:3.9
(python prompt) import wheel
(python prompt) wheel.__version__
```

# Prepare Postgres

Run Postgres and load data as shown in the videos
We'll use the green taxi trips from September 2019:

```wget https://github.com/DataTalksClub/nyc-tlc-data/releases/download/green/green_tripdata_2019-09.csv.gz```

You will also need the dataset with zones:

```wget https://s3.amazonaws.com/nyc-tlc/misc/taxi+_zone_lookup.csv```
wget https://s3.amazonaws.com/nyc-tlc/misc/taxi+_zone_lookup.csv

Download this data and put it into Postgres (with jupyter notebooks or with a pipeline)

**Answer**: The following code was used to prepare postgres connection. Note: the absolute path to data in volume must be in lower-case folder and files names.

```
docker run -it \
  -e POSTGRES_USER="root" \
  -e POSTGRES_PASSWORD="root" \
  -e POSTGRES_DB="ny_taxi" \
  -v d:/2024/programacion/data_engineering_zoomcamp-data_talks/data-engineering-zoomcamp-homework/module1-containerization_and_infrastructure_as_code/ny_taxi_postgres_data:/var/lib/postgresql/data \
  -p 5432:5432 \
  postgres:13
```

After docker setup and when try running `pgcli` I received an *ImportError: no pq wrapper available* message, for which I had to install:

```
pip install psycopg_c
pip install psycopg_binary
```

When try to connect to DB: `pgcli -h localhost -p 5432 -u root -d ny_taxi` received an error after inputed the password for root user: `root`. This problem happen because I already have Postgres installed on my computer, for what should use antoher port connection `-p 5431:5432` while running docker image. I decided to uninstall Postgres anyway and reboot.

After that, I connect to DB without problem with: `pgcli -h localhost -p 5432 -u root -d ny_taxi`, with password: `root`.

To connect pgAdmin and Postgres I use:

```
docker run -it \
  -e PGADMIN_DEFAULT_EMAIL="admin@admin.com" \
  -e PGADMIN_DEFAULT_PASSWORD="root" \
  -p 8080:80 \
  dpage/pgadmin4
```

##### Running Postgres and pgAdmin together

Create a network

```
docker network create pg-network
```

Run Postgres

```
docker run -it \
  -e POSTGRES_USER="root" \
  -e POSTGRES_PASSWORD="root" \
  -e POSTGRES_DB="ny_taxi" \
  -v d:/2024/programacion/data_engineering_zoomcamp-data_talks/data-engineering-zoomcamp-homework/module1-containerization_and_infrastructure_as_code/ny_taxi_postgres_data:/var/lib/postgresql/data \
  -p 5432:5432 \
  --network=pg-network \
  --name pg-database \
  postgres:13
```

Run pgAdmin  (for view in the web browser visit: `localhost:8080`):

```
docker run -it \
  -e PGADMIN_DEFAULT_EMAIL="admin@admin.com" \
  -e PGADMIN_DEFAULT_PASSWORD="root" \
  -p 8080:80 \
  --network=pg-network \
  --name pgadmin-2 \
  dpage/pgadmin4
```

#### Data ingestion (locally)

For this I followed the video lesson *# DE Zoomcamp 1.2.4 - Dockerizing the Ingestion Script* but wiht a slighty different code lines. In the jupyter notebook I modified the file name because in the lesson video the file was a csv format, and when downloaded here it had a gz compression. So there were necessary to add an aditional line in notebook: `os.system(f'gzip -d {csv_compr_name}')` to decompress the file before ingesting into Postgres database. After all I run the following commands in the terminal:

```
URL="https://github.com/DataTalksClub/nyc-tlc-data/releases/download/yellow/yellow_tripdata_2021-01.csv.gz"

python ingest_data.py \
  --user=root \
  --password=root \
  --host=localhost \
  --port=5432 \
  --db=ny_taxi \
  --table_name=yellow_taxi_data \
  --url=${URL}
```

After this I test the existence of the data on the Postgres database, and it was good.

##### Builing the image and run the script with Docker

After writing Dockerfile, to build the image run:

```
docker build -t taxi_ingest:v001 .
```

For running the script with Docker run:

```
URL="https://github.com/DataTalksClub/nyc-tlc-data/releases/download/yellow/yellow_tripdata_2021-01.csv.gz"

docker run -it \
  --network=pg-network \
  taxi_ingest:v001 \
    --user=root \
    --password=root \
    --host=pg-database \
    --port=5432 \
    --db=ny_taxi \
    --table_name=yellow_taxi_data \
    --url=${URL}
```

#### Docker compose

For run it:

```
docker-compose up
```

and in detached mode:

```
docker-compose up -d
```

Shutting down:

```
 docker-compose down
```

Note: to make pgAdmin configuration persistent, create a folder `data_pgadmin`. Change its permission via

```
sudo chown 5050:5050 data_pgadmin
```

and mount it to the `/var/lib/pgadmin` folder:

```yaml
services:
    pgadmin:
        image: dpage/pgadmin4
        volumes:
          - ./data_pgadmin:/var/lib/pgadmin
    ...
```

In order to complement data available I downoladed the file of taxi zones:

```
wget https://s3.amazonaws.com/nyc-tlc/misc/taxi+_zone_lookup.csv
```

in jupyter notebook and save it into a Postgres DB.

Some SQL queries for the next questions:

```sql
-- SELECT
--     *
-- FROM
--     zones
-- LIMIT 100;

-- SELECT
--     *
-- FROM
--     yellow_taxi_data t,
--     zones zpu,
--     zones zdo
-- WHERE 
--     t."PULocationID" = zpu."LocationID" AND
--     t."DOLocationID" = zdo."LocationID"
-- LIMIT 100;

-- SELECT
--     tpep_pickup_datetime,
--     tpep_dropoff_datetime,
--     total_amount,
--     CONCAT(zpu."Borough", ' / ', zpu."Zone") AS "pickup_loc",
--     CONCAT(zdo."Borough", ' / ', zdo."Zone") AS "dropoff_loc"
-- FROM
--     yellow_taxi_data t,
--     zones zpu,
--     zones zdo
-- WHERE 
--     t."PULocationID" = zpu."LocationID" AND
--     t."DOLocationID" = zdo."LocationID"
-- LIMIT 100;

SELECT
    tpep_pickup_datetime,
    tpep_dropoff_datetime,
    total_amount,
    CONCAT(zpu."Borough", ' / ', zpu."Zone") AS "pickup_loc",
    CONCAT(zdo."Borough", ' / ', zdo."Zone") AS "dropoff_loc"
FROM
    yellow_taxi_data t JOIN zones zpu
        ON t."PULocationID" = zpu."LocationID"
    JOIN zones zdo
        ON t."DOLocationID" = zdo."LocationID"
LIMIT 100;
```

## Question 3. Count records

How many taxi trips were totally made on September 18th 2019?

Tip: started and finished on 2019-09-18. 

Remember that `lpep_pickup_datetime` and `lpep_dropoff_datetime` columns are in the format timestamp (date and hour+min+sec) and not in date.

- 15767
- 15612
- 15859
- 89009

**Note**: While uploading csv file, it was in a `gz`format, that should be decompressed with: `gzip -d file.gz`.

**Answer**: for this I query:

```sql
SELECT
    COUNT(*) AS total_trips
FROM
    green_taxi_data t
WHERE
    lpep_pickup_datetime >= '2019-09-18 00:00:00' 
    AND lpep_dropoff_datetime <= '2019-09-18 23:59:59';
```

And the result is: **15612.**

## Question 4. Largest trip for each day

Which was the pick up day with the largest trip distance
Use the pick up time for your calculations.

- 2019-09-18
- 2019-09-16
- 2019-09-26
- 2019-09-21

**Answer**: for this I query:

```sql
SELECT
    CAST(t.lpep_pickup_datetime AS DATE) as pickup_day,
    MAX(t.trip_distance) as max_distance
FROM
    green_taxi_data t
GROUP BY
    pickup_day
ORDER BY
    max_distance DESC;ce DESC;
```

And the result is: **2019-09-26**.

## Question 5. The number of passengers

Consider lpep_pickup_datetime in '2019-09-18' and ignoring Borough has Unknown

Which were the 3 pick up Boroughs that had a sum of total_amount superior to 50000?

- "Brooklyn" "Manhattan" "Queens"
- "Bronx" "Brooklyn" "Manhattan"
- "Bronx" "Manhattan" "Queens" 
- "Brooklyn" "Queens" "Staten Island"

**Answer**: for this I query:

```sql
SELECT
    z."Borough",
    SUM(t.total_amount) AS total_amount
FROM
    green_taxi_data t
JOIN
    zones z ON t."PULocationID" = z."LocationID"
WHERE
    t.lpep_pickup_datetime >= '2019-09-18 00:00:00'
    AND t.lpep_pickup_datetime <= '2019-09-18 23:59:59'
    AND z."Borough" <> 'Unknown'
GROUP BY
    z."Borough"
HAVING
    SUM(total_amount) > 50000
ORDER BY
    total_amount DESC
LIMIT 3;
```

And the result is: **"Brooklyn" "Manhattan" "Queens"**.

## Question 6. Largest tip

For the passengers picked up in September 2019 in the zone name Astoria which was the drop off zone that had the largest tip?
We want the name of the zone, not the id.

Note: it's not a typo, it's `tip` , not `trip`

- Central Park
- Jamaica
- JFK Airport
- Long Island City/Queens Plaza

**Answer**: for this I query:

```sql
SELECT
    zpu."Zone" AS pickup_zone,
    zdo."Zone" AS dropoff_zone,
    MAX(t.tip_amount) AS max_tip
FROM
    green_taxi_data t
JOIN
    zones zpu ON t."PULocationID" = zpu."LocationID"
JOIN
    zones zdo ON t."DOLocationID" = zdo."LocationID"
WHERE
    EXTRACT(YEAR FROM t.lpep_pickup_datetime) = '2019'
    AND EXTRACT(MONTH FROM t.lpep_pickup_datetime) = '09'
    AND zpu."Zone"='Astoria'
GROUP BY
    pickup_zone,
    dropoff_zone
ORDER BY
    max_tip DESC;
```

And the result is: **JFK Airport**.

## Terraform

In this section homework we'll prepare the environment by creating resources in GCP with Terraform.

In your VM on GCP/Laptop/GitHub Codespace install Terraform. 
Copy the files from the course repo
[here](https://github.com/DataTalksClub/data-engineering-zoomcamp/tree/main/week_1_basics_n_setup/1_terraform_gcp/terraform) to your VM/Laptop/GitHub Codespace.

Modify the files as necessary to create a GCP Bucket and Big Query Dataset.

Some commands:

```
terraform init
terraform plan
terraform apply
terraform destroy (importan! run after every session)
```

## Question 7. Creating Resources

After updating the main.tf and variable.tf files run:

```
terraform apply
```

Paste the output of this command into the homework submission form.

**Answer**:

```
google_bigquery_dataset.dataset: Creating...
google_storage_bucket.data-lake-bucket: Creating...
google_storage_bucket.data-lake-bucket: Creation complete after 1s [id=data_lake_bucket_zeta-serenity-412422]
google_bigquery_dataset.dataset: Creation complete after 3s [id=projects/zeta-serenity-412422/datasets/ny_trips_data]
```

## Submitting the solutions

* Form for submitting: https://courses.datatalks.club/de-zoomcamp-2024/homework/hw01
* You can submit your homework multiple times. In this case, only the last submission will be used. 

Deadline: 29 January, 23:00 CET