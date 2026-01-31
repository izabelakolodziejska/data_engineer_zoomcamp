# My solutions to second homework of data zoomcamp course (2026 edition)
To solve this task i had to create kestra workflow that loaded taxi data into postgres database (using scheduler)

## Question 1. Within the execution for Yellow Taxi data for the year 2020 and month 12: what is the uncompressed file size (i.e. the output file yellow_tripdata_2020-12.csv of the extract task)?
Executions -> Outputs -> extract -> outputFiles -> yellow_tripdata_2020-12.csv 
128.3 MiB


## Question 2. What is the rendered value of the variable file when the inputs taxi is set to green, year is set to 2020, and month is set to 04 during execution?
Executions -> Logs -> extract 
green_tripdata_2020-04.csv


## Question 3. How many rows are there for the Yellow Taxi data for all CSV files in the year 2020?
``` sql
select count(*) from yellow_tripdata  where filename like 'yellow_tripdata_2020-%'
```

## Question 4. How many rows are there for the Green Taxi data for all CSV files in the year 2020?
``` sql
select count(*) from green_tripdata  where filename like 'green_tripdata_2020-%'
```
## Question 5. How many rows are there for the Yellow Taxi data for the March 2021 CSV file?
``` sql
select count(*) from yellow_tripdata  where filename like 'yellow_tripdata_2021-03%'
```
