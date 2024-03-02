--Uncomment for run

-- Question 3
-- select count(*) from {{ ref('fact_fhv_trips') }}
-- answer: 22998722

-- Question 4
with green_yellow as (
    select service_type, count(*) as total_records
    from {{ ref('fact_trips') }}
    -- where extract(year from pickup_datetime) = 2019
    -- and extract(month from pickup_datetime) = 10
    group by 1
),
fhv as (
    select service_type, count(*) as total_records
    from {{ ref('fact_fhv_trips') }}
    -- where extract(year from pickup_datetime) = 2019
    -- and extract(month from pickup_datetime) = 10
    group by 1
)

select * from green_yellow
union all
select * from fhv
-- Answer: Fhv. But the real answer is Yellow. I have an error in constructing green_yellow table. The results are not consistent with the solution. If I filter the
-- data by datetime (july 2019) the green_yellow table is empty. It's a matter of availability of this data, but I think the code lines are correct.