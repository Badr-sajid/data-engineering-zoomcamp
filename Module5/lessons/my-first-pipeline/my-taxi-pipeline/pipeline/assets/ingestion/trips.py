"""@bruin

name: ingestion.trips
connection: duckdb-default

materialization:
  type: table
  strategy: append
image: python:3.11

columns:
  - name: vendor_id
    type: INTEGER
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
  - name: taxi_type
    type: VARCHAR
  - name: extracted_at
    type: TIMESTAMP

@bruin"""

import os
import json
import pandas as pd

def materialize():
    start_date = os.getenv("BRUIN_START_DATE")
    end_date = os.getenv("BRUIN_END_DATE")
    taxi_types = json.loads(os.environ["BRUIN_VARS"]).get("taxi_types", ["yellow"])

    months = pd.date_range(start=start_date, end=end_date, freq='MS').strftime("%Y-%m").tolist()
    dfs = []
    for taxi_type in taxi_types:
        for month in months:
            url = f"https://d37ci6vzurychx.cloudfront.net/trip-data/{taxi_type}_tripdata_{month}.parquet"
            df = pd.read_parquet(url)
            df["extracted_at"] = pd.Timestamp.now()
            df["taxi_type"] = taxi_type
            dfs.append(df)
    final_dataframe = pd.concat(dfs, ignore_index=True)
    return final_dataframe
