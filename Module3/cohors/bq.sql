CREATE OR REPLACE EXTERNAL TABLE dtc-de-course-483922.zoomcamp.external_yellow_datatrip
OPTIONS (
  format = 'Parquet',
  uris = ['gs://gcp-zoomcamp-sbadr/yellow_tripdata_2024-0*.parquet']
);

CREATE OR REPLACE TABLE dtc-de-course-483922.zoomcamp.yellow_datatrip_non_partitioned AS
SELECT * FROM dtc-de-course-483922.zoomcamp.external_yellow_datatrip;

SELECT count(1) FROM dtc-de-course-483922.zoomcamp.external_yellow_datatrip;

SELECT count(distinct PULocationID) FROM dtc-de-course-483922.zoomcamp.external_yellow_datatrip;
SELECT count(distinct PULocationID) FROM dtc-de-course-483922.zoomcamp.yellow_datatrip_non_partitioned;

SELECT PULocationID FROM dtc-de-course-483922.zoomcamp.yellow_datatrip_non_partitioned;
SELECT PULocationID, DOLocationID FROM dtc-de-course-483922.zoomcamp.yellow_datatrip_non_partitioned;

SELECT count(1) from dtc-de-course-483922.zoomcamp.yellow_datatrip_non_partitioned
WHERE fare_amount=0;

CREATE OR REPLACE TABLE dtc-de-course-483922.zoomcamp.yellow_datatrip_partitioned_clustered
PARTITION BY DATE(tpep_dropoff_datetime)
CLUSTER BY VendorID 
AS
SELECT * FROM dtc-de-course-483922.zoomcamp.external_yellow_datatrip;

select distinct VendorID from dtc-de-course-483922.zoomcamp.yellow_datatrip_non_partitioned
where tpep_dropoff_datetime between '2024-03-01' and '2024-03-15';

select distinct VendorID from dtc-de-course-483922.zoomcamp.yellow_datatrip_partitioned_clustered
where tpep_dropoff_datetime between '2024-03-01' and '2024-03-15';