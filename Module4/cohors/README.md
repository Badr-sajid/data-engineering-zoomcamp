## Question 1. 
Answer : int_trips_unioned only

## Question 2. 
Answer : dbt will fail the test, returning a non-zero exit code

## Question 3. 
Answer : 12,184

## Question 4. 
Answer : East Harlem North
select pickup_zone, cast(date_trunc(revenue_month, year) as date) as year, revenue_monthly_total_amount
from {{ ref('fct_monthly_zone_revenue') }}
where cast(date_trunc(revenue_month, year) as date) = '2020-01-01' and service_type = 'Green'
order by revenue_monthly_total_amount desc

## Question 5. 
Answer : 384,624
select sum(total_monthly_trips)
from {{ ref('fct_monthly_zone_revenue') }}
where revenue_month = '2019-10-01' and service_type = 'Green'

## Question 6. 
Answer : 43,244,693

CREATE OR REPLACE EXTERNAL TABLE dtc-de-course-483922.zoomcamp.external_fhv_datatrip
OPTIONS (
  format = 'CSV',
  uris = ['gs://gcp-zoomcamp-sbadr/fhv_tripdata_2019-*.csv']
);
CREATE OR REPLACE TABLE dtc-de-course-483922.zoomcamp.fhv_datatrip_partitioned
PARTITION BY DATE(dropOff_datetime)
AS
SELECT * FROM dtc-de-course-483922.zoomcamp.external_fhv_datatrip;