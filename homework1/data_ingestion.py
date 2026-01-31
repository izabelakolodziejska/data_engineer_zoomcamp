from sqlalchemy import create_engine
from tqdm.auto import tqdm
import pandas as pd
import click 

dtype = {
    "LocationID": "Int64",
    "Borough": "string",
    "Zone": "string",
    "service_zone": "string",
    }

dtypes = {
    "VendorID": "Int64",
    "store_and_fwd_flag": "string",
    "RatecodeID": "Float64",
    "PULocationID": "Int64",
    "DOLocationID": "Int64",
    "passenger_count": "Float64",
    "trip_distance": "Float64",
    "fare_amount": "Float64",
    "extra": "Float64",
    "mta_tax": "Float64",
    "tip_amount": "Float64",
    "tolls_amount": "Float64",
    "ehail_fee": "Float64",
    "improvement_surcharge": "Float64",
    "total_amount": "Float64",
    "payment_type": "Float64",
    "trip_type": "Float64",
    "congestion_surcharge": "Float64",
    "cbd_congestion_fee": "Float64",
}

parse_dates = [
    "lpep_pickup_datetime",
    "lpep_dropoff_datetime"
]

@click.command()
@click.option('--pg-user', default='root', help='PostgreSQL user')
@click.option('--pg-pass', default='root', help='PostgreSQL password')
@click.option('--pg-host', default='localhost', help='PostgreSQL host')
@click.option('--pg-port', default=5432, type=int, help='PostgreSQL port')
@click.option('--pg-db', default='green_taxi', help='PostgreSQL database name')
@click.option('--chunksize', default=100000, type=int, help='Chunk size for reading CSV')
def run(pg_user, pg_pass, pg_host, pg_port, pg_db, chunksize):

    engine = create_engine(f'postgresql://{pg_user}:{pg_pass}@{pg_host}:{pg_port}/{pg_db}')

    df_iter = pd.read_csv(
        'data/taxi_zone_lookup.csv',
        dtype=dtype,
        iterator=True,
        chunksize=chunksize,
    )

    first = True

    for df_chunk in tqdm(df_iter):
        if first:
            df_chunk.head(0).to_sql(
                name='taxi_zone',
                con=engine,
                if_exists='replace'
            )
            first = False

        df_chunk.to_sql(
            name='taxi_zone',
            con=engine,
            if_exists='append'
        )


    df_parquet = pd.read_parquet("data/green_tripdata_2025-11.parquet")

    df_parquet = df_parquet.astype(dtypes)

    for col in parse_dates:
        df_parquet[col] = pd.to_datetime(df_parquet[col])

    df_parquet.to_sql(
    name='tripdata',
    con=engine,
    if_exists='replace',
    index=False
)
    


if __name__ == '__main__':
    run()