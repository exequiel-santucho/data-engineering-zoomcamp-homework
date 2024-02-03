## Week 2 Homework - Exequiel Santucho

To perform this task, the following resources have been followed:

- Data Engineering Zoomcamp - 02-workflow-orchestration notes: [Link](https://github.com/DataTalksClub/data-engineering-zoomcamp/tree/main/02-workflow-orchestration)
  
  From above, consulted videos are listed:
  
  - Intro to Orchestration
    
    - [What is Orchestration?](https://www.youtube.com/watch?v=Li8-MWHhTbo&list=PL3MmuxUbc_hJed7dXYoJw8DoCuVHhGEQb)
  
  - Intro to Mage
    
    - [What is Mage?](https://www.youtube.com/watch?v=AicKRcK3pa4&list=PL3MmuxUbc_hJed7dXYoJw8DoCuVHhGEQb)
    
    - [Configuring Mage](https://www.youtube.com/watch?v=tNiV7Wp08XE?list=PL3MmuxUbc_hJed7dXYoJw8DoCuVHhGEQb)
    
    - [A simple Pipeline](https://www.youtube.com/watch?v=stI-gg4QBnI&list=PL3MmuxUbc_hJed7dXYoJw8DoCuVHhGEQb)
  
  - ETL: API to Postgres
    
    - [Configuring Postgres](https://www.youtube.com/watch?v=pmhI-ezd3BE&list=PL3MmuxUbc_hJed7dXYoJw8DoCuVHhGEQb)
    
    - [Writing an ETL Pipeline](https://www.youtube.com/watch?v=Maidfe7oKLs&list=PL3MmuxUbc_hJed7dXYoJw8DoCuVHhGEQb)
  
  - ETL: API to GCS
    
    - [Configuring GCP](https://www.youtube.com/watch?v=00LP360iYvE&list=PL3MmuxUbc_hJed7dXYoJw8DoCuVHhGEQb)
    
    - [Writing an ETL Pipeline](https://www.youtube.com/watch?v=w0XmcASRUnc&list=PL3MmuxUbc_hJed7dXYoJw8DoCuVHhGEQb)
  
  - ETL: GCS to BigQuery
    
    - [Writing an ETL Pipeline](https://www.youtube.com/watch?v=JKp_uzM-XsM)
  
  - Parametrized Execution
    
    - [Parametrized Execution](https://www.youtube.com/watch?v=H0hWjWxB-rg&list=PL3MmuxUbc_hJed7dXYoJw8DoCuVHhGEQb)
    
    - [Backfills](https://www.youtube.com/watch?v=ZoeC6Ag5gQc&list=PL3MmuxUbc_hJed7dXYoJw8DoCuVHhGEQb)
  
  - Deployment (Optional)
    
    - [Deployment Prerequisites](https://www.youtube.com/watch?v=zAwAX5sxqsg&list=PL3MmuxUbc_hJed7dXYoJw8DoCuVHhGEQb)
    
    - [Google Cloud Premissions](https://www.youtube.com/watch?v=O_H7DCmq2rA&list=PL3MmuxUbc_hJed7dXYoJw8DoCuVHhGEQb)
    
    - [Deployment to Google Cloud - Part 1](https://www.youtube.com/watch?v=9A872B5hb_0&list=PL3MmuxUbc_hJed7dXYoJw8DoCuVHhGEQb)
    
    - [Deployment to Google Cloud - Part 2](https://www.youtube.com/watch?v=0YExsb2HgLI&list=PL3MmuxUbc_hJed7dXYoJw8DoCuVHhGEQb)
  
  - Next Steps
    
    - [Next Steps](https://www.youtube.com/watch?v=uUtj7N0TleQ)

From above, consulted specific codes and instructions:

- [Getting Started Repo](https://github.com/DataTalksClub/data-engineering-zoomcamp/tree/main](https://github.com/mage-ai/mage-zoomcamp)

- [Simple loadinf block](https://github.com/mage-ai/mage-zoomcamp/blob/solutions/magic-zoomcamp/data_loaders/load_nyc_taxi_data.py)

---

> In case you don't get one option exactly, select the closest one 

For the homework, we'll be working with the _green_ taxi dataset located here:

`https://github.com/DataTalksClub/nyc-tlc-data/releases/tag/green/download`

### Assignment

The goal will be to construct an ETL pipeline that loads the data, performs some transformations, and writes the data to a database (and Google Cloud!).

- Create a new pipeline, call it `green_taxi_etl`
- Add a data loader block and use Pandas to read data for the final quarter of 2020 (months `10`, `11`, `12`).
  - You can use the same datatypes and date parsing methods shown in the course.
  - `BONUS`: load the final three months using a for loop and `pd.concat`
- Add a transformer block and perform the following:
  - Remove rows where the passenger count is equal to 0 _or_ the trip distance is equal to zero.
  - Create a new column `lpep_pickup_date` by converting `lpep_pickup_datetime` to a date.
  - Rename columns in Camel Case to Snake Case, e.g. `VendorID` to `vendor_id`.
  - Add three assertions:
    - `vendor_id` is one of the existing values in the column (currently)
    - `passenger_count` is greater than 0
    - `trip_distance` is greater than 0
- Using a Postgres data exporter (SQL or Python), write the dataset to a table called `green_taxi` in a schema `mage`. Replace the table if it already exists.
- Write your data as Parquet files to a bucket in GCP, partioned by `lpep_pickup_date`. Use the `pyarrow` library!
- Schedule your pipeline to run daily at 5AM UTC.

### Questions

## Question 1. Data Loading

Once the dataset is loaded, what's the shape of the data?

* 266,855 rows x 20 columns
* 544,898 rows x 18 columns
* 544,898 rows x 20 columns
* 133,744 rows x 20 columns

**Answer**: 266,855 rows x 20 columns

## Question 2. Data Transformation

Upon filtering the dataset where the passenger count is greater than 0 _and_ the trip distance is greater than zero, how many rows are left?

* 544,897 rows
* 266,855 rows
* 139,370 rows
* 266,856 rows

**Answer**: 139,370 rows

## Question 3. Data Transformation

Which of the following creates a new column `lpep_pickup_date` by converting `lpep_pickup_datetime` to a date?

* `data = data['lpep_pickup_datetime'].date`
* `data('lpep_pickup_date') = data['lpep_pickup_datetime'].date`
* `data['lpep_pickup_date'] = data['lpep_pickup_datetime'].dt.date`
* `data['lpep_pickup_date'] = data['lpep_pickup_datetime'].dt().date()`

**Answer**: `data['lpep_pickup_date'] = data['lpep_pickup_datetime'].dt.date`

## Question 4. Data Transformation

What are the existing values of `VendorID` in the dataset?

* 1, 2, or 3
* 1 or 2
* 1, 2, 3, 4
* 1

**Answer**: 1 or 2

## Question 5. Data Transformation

How many columns need to be renamed to snake case?

* 3
* 6
* 2
* 4

**Answer**: 4

## Question 6. Data Exporting

Once exported, how many partitions (folders) are present in Google Cloud?

* 96
* 56
* 67
* 108

**Answer**: 96. Actually the solution obtained is 95, but based on the initial recommendation (to choose the closest value), 96 is chosen.

## Submitting the solutions

* Form for submitting: https://courses.datatalks.club/de-zoomcamp-2024/homework/hw2
* Check the link above to see the due date

## Solution

<img title="" src="https://raw.githubusercontent.com/exequiel-santucho/data-engineering-zoomcamp-homework/master/02-workflow-orchestration/mage-homework-blocks/etl_pipeline.PNG" alt="" data-align="center">

1) **Code block for Question 1 (Data Loader)**

```python
import io
import pandas as pd
import requests
if 'data_loader' not in globals():
    from mage_ai.data_preparation.decorators import data_loader
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test


@data_loader
def load_data_from_api(*args, **kwargs):
    """
    Template for loading data from API
    """
    urls = ['https://github.com/DataTalksClub/nyc-tlc-data/releases/download/green/green_tripdata_2020-10.csv.gz',
            'https://github.com/DataTalksClub/nyc-tlc-data/releases/download/green/green_tripdata_2020-11.csv.gz',
            'https://github.com/DataTalksClub/nyc-tlc-data/releases/download/green/green_tripdata_2020-12.csv.gz'
    ]

    taxi_dtypes = {
                    'VendorID': pd.Int64Dtype(),
                    'passenger_count': pd.Int64Dtype(),
                    'trip_distance': float,
                    'RatecodeID':pd.Int64Dtype(),
                    'store_and_fwd_flag':str,
                    'PULocationID':pd.Int64Dtype(),
                    'DOLocationID':pd.Int64Dtype(),
                    'payment_type': pd.Int64Dtype(),
                    'fare_amount': float,
                    'extra':float,
                    'mta_tax':float,
                    'tip_amount':float,
                    'tolls_amount':float,
                    'improvement_surcharge':float,
                    'total_amount':float,
                    'congestion_surcharge':float
                }

    # native date parsing 
    parse_dates = ['lpep_pickup_datetime', 'lpep_dropoff_datetime']

    # read 3 files
    li = []

    for url in urls:
        df_i = pd.read_csv(url, sep=',', compression='gzip', dtype=taxi_dtypes, parse_dates=parse_dates)
        li.append(df_i)

    df = pd.concat(li, axis=0, ignore_index=True)
    print(f'The shape of the dataframe loaded is: {df.shape[0]} rows and {df.shape[1]} columns')

    return df


@test
def test_output(output, *args) -> None:
    """
    Template code for testing the output of the block.
    """
    assert output is not None, 'The output is undefined'
```

2) **Code block for Questions 2, 3, 4 and 5 (Transformer)**

```python
if 'transformer' not in globals():
    from mage_ai.data_preparation.decorators import transformer
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test


@transformer
def transform(data, *args, **kwargs):

    # print('Rows with zero passengers: ', data['passenger_count'].isin([0]).sum())
    # print('Rows with trip distance graeter than zero: ', data['trip_distance'].gt(0).sum())

    # Question 2
    df = data[(data['passenger_count'] > 0) & (data['trip_distance'] > 0)]
    print(f'The shape of the transformed dataframe is: {df.shape[0]} rows and {df.shape[1]} columns')

    # Question 3
    df['lpep_pickup_date'] = df['lpep_pickup_datetime'].dt.date
    print(df[['lpep_pickup_datetime', 'lpep_pickup_date']].dtypes)

    # Question 4
    print('Existing values for VendorID :', df['VendorID'].unique())

    # Question 5
    def is_camel_case(s):
        return s != s.lower() and s != s.upper() and "_" not in s

    cols = df.columns.to_list()
    n = 0
    for col in cols:
        if is_camel_case(col):
            n += 1

    print('Number of columns that need to be renamed to snake case: ', n)

    # transform cols CamelCase to snake_case
    df.columns = (df.columns
                .str.replace('(?<=[a-z])(?=[A-Z])', '_', regex=True)
                .str.lower()
             )

    return df


@test
def test_1_output(output, *args):
    # vendor_id (snake_case) is in the columns of df
    assert 'vendor_id' in output.columns, '"vendor_id" column is not in df'

@test
def test_2_output(output, *args):
    # passenger_count is greater than 0
    assert output['passenger_count'].gt(0).any(), 'There are rides with zero passengers'

@test
def test_3_output(output, *args):
    # trip_distance is greater than 0
    assert output['trip_distance'].gt(0).any(), 'There are rides with zero trip distances'
```

3) **Blocks for Question 6 (Data Exporter)**
   
   Data to Postgres:
   
   ```python
   from mage_ai.settings.repo import get_repo_path
   from mage_ai.io.config import ConfigFileLoader
   from mage_ai.io.postgres import Postgres
   from pandas import DataFrame
   from os import path
   
   if 'data_exporter' not in globals():
       from mage_ai.data_preparation.decorators import data_exporter
   ```

   @data_exporter
   def export_data_to_postgres(df: DataFrame, **kwargs) -> None:
       """
       Template for exporting data to a PostgreSQL database.
       Specify your configuration settings in 'io_config.yaml'.

       Docs: https://docs.mage.ai/design/data-loading#postgresql
       """
       schema_name = 'mage'  # Specify the name of the schema to export data to
       table_name = 'green_taxi'  # Specify the name of the table to export data to
       config_path = path.join(get_repo_path(), 'io_config.yaml')
       config_profile = 'dev'
    
       with Postgres.with_config(ConfigFileLoader(config_path, config_profile)) as loader:
           loader.export(
               df,
               schema_name,
               table_name,
               index=False,  # Specifies whether to include index in exported table
               if_exists='replace',  # Specify resolution policy if table name already exists
           )

```
Partitioned parquet data to GCS:

```python
import pyarrow as pa
import pyarrow.parquet as pq
import os


if 'data_exporter' not in globals():
 from mage_ai.data_preparation.decorators import data_exporter

os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = '/home/src/zeta-serenity-412422-0196fa2482b7.json'

bucket_name = 'mage_zoomcamp_xqlsnt'
project_id = 'zeta-serenity-412422'

table_name = 'ny_green_taxi_data'

root_path = f'{bucket_name}/{table_name}'

@data_exporter
def export_data(data, *args, **kwargs):

 table = pa.Table.from_pandas(data)

 gcs = pa.fs.GcsFileSystem()

 pq.write_to_dataset(
     table,
     root_path=root_path,
     partition_cols=['lpep_pickup_date'],
     filesystem=gcs
 )
```

Single parquet data to GCS (not required in this homework):

```python
from mage_ai.settings.repo import get_repo_path
from mage_ai.io.config import ConfigFileLoader
from mage_ai.io.google_cloud_storage import GoogleCloudStorage
from pandas import DataFrame
from os import path

if 'data_exporter' not in globals():
    from mage_ai.data_preparation.decorators import data_exporter


@data_exporter
def export_data_to_google_cloud_storage(df: DataFrame, **kwargs) -> None:
    """
    Template for exporting data to a Google Cloud Storage bucket.
    Specify your configuration settings in 'io_config.yaml'.

    Docs: https://docs.mage.ai/design/data-loading#googlecloudstorage
    """
    config_path = path.join(get_repo_path(), 'io_config.yaml')
    config_profile = 'default'

    bucket_name = 'mage_zoomcamp_xqlsnt'
    object_key = 'ny_green_taxi_data.parquet'

    GoogleCloudStorage.with_config(ConfigFileLoader(config_path, config_profile)).export(
        df,
        bucket_name,
        object_key,
    )
```
