# My solutions to first homework of data zoomcamp course (2026 edition)
Regarding question 7, i did those steps in https://github.com/izabelakolodziejska/data-engineer-zoomcamp-01
## Question 1. Understanding Docker images
Commands used:
```console
docker run -it --entrypoint bash python:3.13
pip --version
```

## Steps i did in terminal to load data into Postgres database (i skipped installing dependencies and adding them to uv)
```console
pip install uv
uv init --python=3.13
docker network create pg-network

docker run -it \
  -e POSTGRES_USER="root" \
  -e POSTGRES_PASSWORD="root" \
  -e POSTGRES_DB="green_taxi" \
  -v postgres_data:/var/lib/postgresql \
  -p 5432:5432 \
  --network=pg-network \
  --name pgdatabase \
  postgres:18


docker run -it \
  -e PGADMIN_DEFAULT_EMAIL="admin@admin.com" \
  -e PGADMIN_DEFAULT_PASSWORD="root" \
  -v pgadmin_data:/var/lib/pgadmin \
  -p 8085:80 \
  --network=pg-network \
  --name pgadmin \
  dpage/pgadmin4


uv run python data_ingestion.py \
  --pg-user=root \
  --pg-pass=root \
  --pg-host=localhost \
  --pg-port=5432 \
  --pg-db=green_taxi \
```

## Question 3. Counting short trips
``` sql
select count(*)
from tripdata
where lpep_pickup_datetime between '2025-11-01' and '2025-12-01' and trip_distance <= 1
```
## Question 4. Longest trip for each day
``` sql
select lpep_pickup_datetime
from tripdata
where trip_distance < 100
order by trip_distance desc
limit 1
```

## Question 5. Biggest pickup zone
``` sql
select count(*), z."Zone"
from tripdata t
inner join taxi_zone z
ON t."PULocationID" = z."LocationID"
group by t."PULocationID", z."Zone"
order by count(*) desc
limit 1
```
## Question 6. Largest tip
``` sql
select z."Zone"
from tripdata t
inner join taxi_zone z
ON t."PULocationID" = z."LocationID"
where z."Zone" = 'East Harlem North'
and t.lpep_pickup_datetime >= '2025-11-01'
and t.lpep_pickup_datetime < '2025-12-01'
order by tip_amount desc
limit 1
```
