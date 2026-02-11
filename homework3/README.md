``` sql
create or replace external table `taxi_data.yellow_tripdata_raw`
options (
  format = 'Parquet',
  uris = ['gs://homework_storage/yellow_tripdata_*.parquet']
);

create or replace table taxi_data.yellow_tripdata
as
select * from taxi_data.yellow_tripdata_raw;

select * from `taxi_data.yellow_tripdata` limit 10;

--Question 1
select count(*) from `taxi_data.yellow_tripdata`;
--20332093	

--Question 2
select count(distinct PULocationID) from `taxi_data.yellow_tripdata_raw`;
--155.12 MB
select count(distinct PULocationID) from `taxi_data.yellow_tripdata`;
--0 MB

--Question 3
select PULocationID  from `taxi_data.yellow_tripdata`;
--155.12 MB

select PULocationID, DOLocationID   from `taxi_data.yellow_tripdata`;
--310.24  MB

--Question 4
select count(*) from `taxi_data.yellow_tripdata`
where fare_amount = 0;
--8333

--Question 5
create or replace table taxi_data.yellow_tripdata_partitioned_clustered
partition by date(tpep_dropoff_datetime)
cluster by VendorID as
select * from taxi_data.yellow_tripdata;

--Question 6
select distinct VendorID from `taxi_data.yellow_tripdata`
where tpep_dropoff_datetime between '2024-03-01' and '2024-03-15';
--310.24 MB

select distinct VendorID from `taxi_data.yellow_tripdata_partitioned_clustered`
where tpep_dropoff_datetime between '2024-03-01' and '2024-03-15';
--26.84 MB
```