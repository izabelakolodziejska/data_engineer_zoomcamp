"""@bruin

name: ingestion.trips
type: python
image: python:3.11
connection: gcp-default

materialization:
  type: table
  strategy: append

columns:
  - name: VendorID
    type: integer
    description: A code indicating the TPEP provider
  - name: tpep_pickup_datetime
    type: timestamp
    description: The date and time when the meter was engaged (yellow taxi)
  - name: lpep_pickup_datetime
    type: timestamp
    description: The date and time when the meter was engaged (green taxi)
  - name: tpep_dropoff_datetime
    type: timestamp
    description: The date and time when the meter was disengaged (yellow taxi)
  - name: lpep_dropoff_datetime
    type: timestamp
    description: The date and time when the meter was disengaged (green taxi)
  - name: passenger_count
    type: integer
    description: The number of passengers in the vehicle
  - name: trip_distance
    type: float
    description: The elapsed trip distance in miles
  - name: RatecodeID
    type: integer
    description: The final rate code in effect at the end of the trip
  - name: store_and_fwd_flag
    type: string
    description: This flag indicates whether the trip record was held in vehicle memory
  - name: PULocationID
    type: integer
    description: TLC Taxi Zone in which the taximeter was engaged
  - name: DOLocationID
    type: integer
    description: TLC Taxi Zone in which the taximeter was disengaged
  - name: payment_type
    type: integer
    description: A numeric code signifying how the passenger paid for the trip
  - name: fare_amount
    type: float
    description: The time-and-distance fare calculated by the meter
  - name: extra
    type: float
    description: Miscellaneous extras and surcharges
  - name: mta_tax
    type: float
    description: MTA tax that is automatically triggered
  - name: tip_amount
    type: float
    description: Tip amount
  - name: tolls_amount
    type: float
    description: Total amount of all tolls paid in trip
  - name: improvement_surcharge
    type: float
    description: Improvement surcharge assessed
  - name: total_amount
    type: float
    description: The total amount charged to passengers
  - name: congestion_surcharge
    type: float
    description: Congestion surcharge amount
  - name: airport_fee
    type: float
    description: Airport fee amount
  - name: taxi_type
    type: string
    description: Type of taxi (yellow or green)
  - name: extracted_at
    type: timestamp
    description: Timestamp when data was extracted

@bruin"""

import pandas as pd
import requests
from datetime import datetime
from dateutil.relativedelta import relativedelta
import os
import json


def materialize():
    """
    Fetch NYC Taxi trip data from TLC public endpoint.
    
    Uses:
    - BRUIN_START_DATE/BRUIN_END_DATE: Date range for data extraction
    - BRUIN_VARS: Pipeline variables including taxi_types
    
    Returns:
    - DataFrame with raw taxi trip data plus metadata columns
    """
    # Read Bruin environment variables
    start_date = datetime.strptime(os.environ['BRUIN_START_DATE'], '%Y-%m-%d')
    end_date = datetime.strptime(os.environ['BRUIN_END_DATE'], '%Y-%m-%d')
    
    # Parse pipeline variables to get taxi types
    bruin_vars = json.loads(os.environ.get('BRUIN_VARS', '{}'))
    taxi_types = bruin_vars.get('taxi_types', ['yellow'])
    
    # Base URL for NYC Taxi data
    base_url = "https://d37ci6vzurychx.cloudfront.net/trip-data/"
    
    # Generate list of URLs to fetch
    dataframes = []
    current_date = start_date
    
    print(f"Fetching data from {start_date.strftime('%Y-%m')} to {end_date.strftime('%Y-%m')}")
    print(f"Taxi types: {taxi_types}")
    
    while current_date <= end_date:
        year_month = current_date.strftime('%Y-%m')
        
        for taxi_type in taxi_types:
            filename = f"{taxi_type}_tripdata_{year_month}.parquet"
            url = base_url + filename
            
            try:
                print(f"Downloading {filename}...")
                response = requests.get(url, timeout=300)
                response.raise_for_status()
                
                # Read parquet data directly from bytes
                df = pd.read_parquet(pd.io.common.BytesIO(response.content))
                
                # Add metadata columns
                df['taxi_type'] = taxi_type
                df['extracted_at'] = datetime.now()
                
                dataframes.append(df)
                print(f"✓ Successfully fetched {filename} ({len(df)} rows)")
                
            except requests.exceptions.RequestException as e:
                print(f"✗ Failed to fetch {filename}: {e}")
                # Continue with other files even if one fails
                continue
        
        # Move to next month
        current_date += relativedelta(months=1)
    
    if not dataframes:
        raise ValueError("No data was successfully fetched")
    
    # Concatenate all dataframes
    final_df = pd.concat(dataframes, ignore_index=True)
    
    print(f"\nTotal rows fetched: {len(final_df)}")
    print(f"Columns: {list(final_df.columns)}")
    
    return final_df


