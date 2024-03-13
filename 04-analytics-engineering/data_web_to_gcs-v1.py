import io
import os
import requests
import pandas as pd
from google.cloud import storage
# Credits to https://github.com/datadb. Adapted version.

"""
Pre-reqs: 
1. `pip install pandas pyarrow google-cloud-storage`
2. Set GOOGLE_APPLICATION_CREDENTIALS to your project/service-account key
3. Set GCP_GCS_BUCKET as your bucket or change default value of BUCKET
"""
os.environ['GOOGLE_APPLICATION_CREDENTIALS'] ="D:/2024/programacion/GCP_project_creds/zeta-serenity-412422-143c9efa3ae9.json"

# services = ['fhv','green','yellow']
init_url = 'https://github.com/DataTalksClub/nyc-tlc-data/releases/download/'
# switch out the bucketname
BUCKET = os.environ.get("GCP_GCS_BUCKET", "module4-ny-taxi-data")


def upload_to_gcs(bucket, object_name, local_file):
    """
    Ref: https://cloud.google.com/storage/docs/uploading-objects#storage-upload-object-python
    """
    # WORKAROUND to prevent timeout for files > 6 MB on 800 kbps upload speed.
    # (Ref: https://github.com/googleapis/python-storage/issues/74)
    storage.blob._MAX_MULTIPART_SIZE = 5 * 1024 * 1024  # 5 MB
    storage.blob._DEFAULT_CHUNKSIZE = 5 * 1024 * 1024  # 5 MB

    client = storage.Client()
    bucket = client.bucket(bucket)
    blob = bucket.blob(object_name)
    blob.upload_from_filename(local_file)


def web_to_gcs(year, service):
    for i in range(12):
        
        # sets the month part of the file_name string
        month = '0'+str(i+1)
        month = month[-2:]

        # csv file_name
        file_name = f"{service}_tripdata_{year}-{month}.csv.gz"

        # download it using requests via a pandas df
        request_url = f"{init_url}{service}/{file_name}"
        r = requests.get(request_url)
        open(file_name, 'wb').write(r.content)
        print(f"Local: {file_name}")

        #define all the datatypes
        #this was necessary as I was getting field datatype errors when reading in GBQ
        #this enforces the fields coming in to be a certain datatype
        taxi_dtypes = {
                    'VendorID': float,
                    'passenger_count': float,
                    'trip_distance': float,
                    'store_and_fwd_flag': str,
                    'RatecodeID': float,
                    'PULocationID': pd.Int64Dtype(),
                    'PUlocationID': pd.Int64Dtype(),
                    'DOLocationID': pd.Int64Dtype(),
                    'DOlocationID': pd.Int64Dtype(),
                    'payment_type': float,
                    'fare_amount': float,
                    'extra': float,
                    'mta_tax': float,
                    'tip_amount': float,
                    'tolls_amount': float,
                    'ehail_fee': float,
                    'improvement_surcharge': float,
                    'total_amount': float,
                    'congestion_surcharge': float,
                    'Affiliated_base_number': str,
                    'SR_Flag': pd.Int64Dtype(),
                    'dispatching_base_num': str,
                    'trip_type': float,
                }
        
        # native date parsing
        # depending on which file type comes through I do the date parsing by field name 
        if(service=='green'): 
            parse_dates = ['lpep_pickup_datetime', 'lpep_dropoff_datetime']
        elif(service=='yellow'):
            parse_dates = ['tpep_pickup_datetime', 'tpep_dropoff_datetime']
        elif(service=='fhv'):
            parse_dates = ['pickup_datetime','dropOff_datetime']

        # Read the data into an iterator
        # splitting the file into chunks helped in the memory management
        df_iter = pd.read_csv(file_name \
                      , sep=',' \
                      , compression='gzip' \
                      , dtype=taxi_dtypes \
                      , parse_dates=parse_dates \
                      , iterator=True \
                      , chunksize=100000 \
                      , low_memory=False)
        
        #iterate over the chunks and append them to an array
        data=[]
        for batch in df_iter:
            data.append(batch)

        #bring all the chunks of data together
        df = pd.concat(data)

        #output the file as parquet file
        file_name = file_name.replace('.csv.gz', '.parquet')
        # ADDED THE coerce_timestamps="us" PART for appropriate timestamp values in bigquery
        # df.to_parquet(file_name, engine='pyarrow')
        df.to_parquet(file_name, engine='pyarrow', coerce_timestamps="us")
        print(f"Parquet: {file_name}")

        # upload it to gcs 
        upload_to_gcs(BUCKET, f"{service}/{file_name}", file_name)
        print(f"GCS: {service}/{file_name}")

        # remove files from local dir
        os.remove(f"{service}_tripdata_{year}-{month}.csv.gz")
        os.remove(f"{service}_tripdata_{year}-{month}.parquet")

#-----------------------------------------------------------------------
# MAIN PROCESSING SECTION
web_to_gcs('2019', 'green')
web_to_gcs('2020', 'green')
web_to_gcs('2019', 'yellow')
web_to_gcs('2020', 'yellow')
web_to_gcs('2019', 'fhv')

