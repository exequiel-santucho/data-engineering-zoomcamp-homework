## Module 5 Homework - Exequiel Santucho

In this homework we'll put what we learned about Spark in practice.

For this homework we will be using the FHV 2019-10 data found here. [FHV Data](https://github.com/DataTalksClub/nyc-tlc-data/releases/download/fhv/fhv_tripdata_2019-10.csv.gz)

---

*Notes:*

When setting environmental variables and paths I followed this videos:

- [Install Apache PySpark on Windows PC | Apache Spark Installation Guide]([Install Apache PySpark on Windows PC | Apache Spark Installation Guide - YouTube](https://www.youtube.com/watch?v=OmcSTQVkrvo))

- [Setting up PySpark IDE | Installing Anaconda, Jupyter Notebook and Spyder IDE]([Setting up PySpark IDE | Installing Anaconda, Jupyter Notebook and Spyder IDE - YouTube](https://www.youtube.com/watch?v=QbS1XT_D7CM))

I added `PYTHONPATH` variable in the same way that the above videos show. For any reason I couldn't set the variables with the next lines of bash codes:

```bash
# run terminal in dir ".../tools/hadoop-3.2.0"
export JAVA_HOME="/d/2024/programacion/data_engineering_zoomcamp-data_talks/data-engineering-zoomcamp-homework/module5-batch/tools/jdk-11.0.13"
export PATH="${JAVA_HOME}/bin:${PATH}"
```

```bash
export HADOOP_HOME="/d/2024/programacion/data_engineering_zoomcamp-data_talks/data-engineering-zoomcamp-homework/module5-batch/tools/hadoop-3.2.0"
export PATH="${HADOOP_HOME}/bin:${PATH}"
```

```bash
# run terminal in dir ".../tools"
export SPARK_HOME="/d/2024/programacion/data_engineering_zoomcamp-data_talks/data-engineering-zoomcamp-homework/module5-batch/tools/spark-3.3.2-bin-hadoop3"
export PATH="${SPARK_HOME}/bin:${PATH}"
```

Also very important: I use conda environment (`activate base`) with **python 3.10.13** because **python 3.12** was incompatible with version **3.3.2 of spark** with base environment activated I had to run `pip install pyspark==3.3.2`



### Question 1:

**Install Spark and PySpark** 

- Install Spark
- Run PySpark
- Create a local spark session
- Execute spark.version.

What's the output?

> [!NOTE]
> To install PySpark follow this [guide](https://github.com/DataTalksClub/data-engineering-zoomcamp/blob/main/05-batch/setup/pyspark.md)

**Answer**: 3.3.2

### Question 2:

**FHV October 2019**

Read the October 2019 FHV into a Spark Dataframe with a schema as we did in the lessons.

Repartition the Dataframe to 6 partitions and save it to parquet.

What is the average size of the Parquet (ending with .parquet extension) Files that were created (in MB)? Select the answer which most closely matches.

- 1MB
- 6MB
- 25MB
- 87MB

**Answer**: 6MB

### Question 3:

**Count records** 

How many taxi trips were there on the 15th of October?

Consider only trips that started on the 15th of October.

- 108,164
- 12,856
- 452,470
- 62,610

> [!IMPORTANT]
> Be aware of columns order when defining schema

**Answer**: 62610

### Question 4:

**Longest trip for each day** 

What is the length of the longest trip in the dataset in hours?

- 631,152.50 Hours
- 243.44 Hours
- 7.68 Hours
- 3.32 Hours

**Answer**: 631,152.50 Hours

### Question 5:

**User Interface**

Spark’s User Interface which shows the application's dashboard runs on which local port?

- 80
- 443
- 4040
- 8080

**Answer**: 4040

### Question 6:

**Least frequent pickup location zone**

Load the zone lookup data into a temp view in Spark</br>
[Zone Data](https://github.com/DataTalksClub/nyc-tlc-data/releases/download/misc/taxi_zone_lookup.csv)

Using the zone lookup data and the FHV October 2019 data, what is the name of the LEAST frequent pickup location Zone?</br>

- East Chelsea
- Jamaica Bay
- Union Sq
- Crown Heights North

**Answer**: Jamaica Bay

## Submitting the solutions

- Form for submitting: https://courses.datatalks.club/de-zoomcamp-2024/homework/hw5
- Deadline: See the website
- Solution: [DE Zoomcamp 2024 - Homework #5 Solution by Michael Shoemaker - YouTube](https://www.youtube.com/watch?v=YtddC7vJOgQ)
