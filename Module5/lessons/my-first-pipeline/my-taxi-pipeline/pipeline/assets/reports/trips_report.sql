/* @bruin
name: reports.trips_report
type: duckdb.sql
depends:
  - staging.trips

materialization:
  type: table

# TODO: Define report columns + primary key(s) at your chosen level of aggregation.
columns:
  - name: trip_date
    type: date
    primary_key: true
  - name: taxi_type
    type: string
    primary_key: true
  - name: payment_type
    type: string
    primary_key: true
  - name: trip_count
    type: bigint
    checks:
      - name: non_negative
        description: Ensure the trip count is never negative
        query: |
          SELECT COUNT(*) = 0 OR MIN(trip_count) >= 0 FROM reports.trips_report
        value: 1

@bruin */

-- Purpose of reports:
-- - Aggregate staging data for dashboards and analytics
-- Required Bruin concepts:
-- - Filter using `{{ start_datetime }}` / `{{ end_datetime }}` for incremental runs
-- - GROUP BY your dimension + date columns

SELECT
    CAST(tpep_pickup_datetime AS DATE) AS trip_date,
    taxi_type,
    payment_type_name AS payment_type,
    COUNT(*) AS trip_count,
    SUM(fare_amount) AS total_fare,
    AVG(fare_amount) AS avg_fare
FROM staging.trips
WHERE tpep_pickup_datetime >= '{{ start_datetime }}'
  AND tpep_pickup_datetime < '{{ end_datetime }}'
GROUP BY 1, 2, 3

