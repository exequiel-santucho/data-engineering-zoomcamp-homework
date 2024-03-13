{{
    config(
        materialized='view'
    )
}}

select
	dispatching_base_num,

	cast(PUlocationID as integer) as pickup_locationid,
	cast(DOlocationID as integer) as dropoff_locationid,

	-- timestamps
	cast(pickup_datetime as timestamp) as pickup_datetime,
	cast(dropoff_datetime as timestamp) as dropoff_datetime,

	-- trip info
	sr_flag
from {{ source('staging', 'fhv_tripdata') }}
where extract(year from pickup_datetime) = 2019