## Question 1. 
Answer : 20,332,093

## Question 2. 
Answer : 0 MB for the External Table and 155.12 MB for the Materialized Table

## Question 3. 
Answer : BigQuery is a columnar database, and it only scans the specific columns requested in the query. Querying two columns (PULocationID, DOLocationID) requires reading more data than querying one column (PULocationID), leading to a higher estimated number of bytes processed.

## Question 4. 
Answer : 8,333

## Question 5. 
Answer : Partition by tpep_dropoff_datetime and Cluster on VendorID

## Question 6. 
Answer : 310.24 MB for non-partitioned table and 26.84 MB for the partitioned table

## Question 7. 
Answer : GCP Bucket

## Question 8. 
Answer : False