/* @bruin

name: staging.trips
type: duckdb.sql

materialization:
  type: table

depends:
  - ingestion.trips
  - ingestion.payment_lookup

columns:
  - name: vendor_id
    type: INTEGER
  - name: taxi_type
    type: VARCHAR
  - name: tpep_pickup_datetime
    type: TIMESTAMP
  - name: tpep_dropoff_datetime
    type: TIMESTAMP
  - name: passenger_count
    type: DOUBLE
  - name: trip_distance
    type: DOUBLE
  - name: ratecode_id
    type: DOUBLE
  - name: store_and_fwd_flag
    type: VARCHAR
  - name: pu_location_id
    type: INTEGER
  - name: do_location_id
    type: INTEGER
  - name: payment_type
    type: BIGINT
  - name: fare_amount
    type: DOUBLE
  - name: extra
    type: DOUBLE
  - name: mta_tax
    type: DOUBLE
  - name: tip_amount
    type: DOUBLE
  - name: tolls_amount
    type: DOUBLE
  - name: improvement_surcharge
    type: DOUBLE
  - name: total_amount
    type: DOUBLE
  - name: congestion_surcharge
    type: DOUBLE
  - name: airport_fee
    type: DOUBLE
  - name: cbd_congestion_fee
    type: DOUBLE
  - name: payment_type_name
    type: VARCHAR

custom_checks:
  - name: row_count_positive
    description: Ensure the staging table has the expected number of rows
    value: 1
    query: |
      -- TODO: return a single scalar (COUNT(*), etc.) that should match `value`
      SELECT COUNT(*) > 0 FROM staging.trips

@bruin */

SELECT 
  t.vendor_id,
  t.taxi_type,
  t.tpep_pickup_datetime,
  t.tpep_dropoff_datetime,
  t.passenger_count,
  t.trip_distance,
  t.ratecode_id,
  t.store_and_fwd_flag,
  t.pu_location_id,
  t.do_location_id,
  t.payment_type,
  t.fare_amount,
  t.extra,
  t.mta_tax,
  t.tip_amount,
  t.tolls_amount,
  t.improvement_surcharge,
  t.total_amount,
  t.congestion_surcharge,
  t.airport_fee,
  t.cbd_congestion_fee,
  p.payment_type_name
FROM ingestion.trips t 
left join ingestion.payment_lookup p
on t.payment_type = p.payment_type_id
WHERE 
  vendor_id is not null
  AND tpep_pickup_datetime >= '{{ start_datetime }}'
  AND tpep_pickup_datetime < '{{ end_datetime }}'
QUALIFY ROW_NUMBER() OVER (
  PARTITION BY 
    t.vendor_id,
    t.taxi_type,
    t.tpep_pickup_datetime,
    t.tpep_dropoff_datetime,
    t.passenger_count,
    t.trip_distance,
    t.ratecode_id,
    t.store_and_fwd_flag,
    t.pu_location_id,
    t.do_location_id,
    t.payment_type,
    t.fare_amount,
    t.extra,
    t.mta_tax,
    t.tip_amount,
    t.tolls_amount,
    t.improvement_surcharge,
    t.total_amount,
    t.congestion_surcharge,
    t.airport_fee,
    t.cbd_congestion_fee
    ORDER BY tpep_pickup_datetime
  ) = 1
