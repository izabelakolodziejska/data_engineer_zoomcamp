/* @bruin

name: staging.trips
type: bq.sql

depends:
  - ingestion.trips
  - ingestion.payment_lookup

materialization:
  type: table
  strategy: merge

columns:
  - name: trip_id
    type: string
    description: Unique identifier for each trip (composite key hash)
    primary_key: true
    checks:
      - name: not_null
      - name: unique
  - name: vendor_id
    type: integer
    description: A code indicating the TPEP provider
  - name: pickup_datetime
    type: timestamp
    description: The date and time when the meter was engaged
    checks:
      - name: not_null
  - name: dropoff_datetime
    type: timestamp
    description: The date and time when the meter was disengaged
    checks:
      - name: not_null
  - name: passenger_count
    type: integer
    description: The number of passengers in the vehicle
    checks:
      - name: positive
  - name: trip_distance
    type: float
    description: The elapsed trip distance in miles
    checks:
      - name: non_negative
  - name: rate_code_id
    type: integer
    description: The final rate code in effect at the end of the trip
  - name: store_and_fwd_flag
    type: string
    description: This flag indicates whether the trip record was held in vehicle memory
  - name: pickup_location_id
    type: integer
    description: TLC Taxi Zone in which the taximeter was engaged
    checks:
      - name: not_null
  - name: dropoff_location_id
    type: integer
    description: TLC Taxi Zone in which the taximeter was disengaged
    checks:
      - name: not_null
  - name: payment_type
    type: integer
    description: A numeric code signifying how the passenger paid for the trip
    checks:
      - name: not_null
  - name: payment_type_name
    type: string
    description: Human-readable payment type description
  - name: fare_amount
    type: float
    description: The time-and-distance fare calculated by the meter
    checks:
      - name: non_negative
  - name: extra
    type: float
    description: Miscellaneous extras and surcharges
  - name: mta_tax
    type: float
    description: MTA tax that is automatically triggered
  - name: tip_amount
    type: float
    description: Tip amount
    checks:
      - name: non_negative
  - name: tolls_amount
    type: float
    description: Total amount of all tolls paid in trip
    checks:
      - name: non_negative
  - name: improvement_surcharge
    type: float
    description: Improvement surcharge assessed
  - name: total_amount
    type: float
    description: The total amount charged to passengers
    checks:
      - name: non_negative
  - name: congestion_surcharge
    type: float
    description: Congestion surcharge amount
  - name: airport_fee
    type: float
    description: Airport fee amount
  - name: taxi_type
    type: string
    description: Type of taxi (yellow or green)
    checks:
      - name: not_null

custom_checks:
  - name: row_count_positive
    description: Ensures table is not empty
    query: |
      SELECT COUNT(*) > 0 FROM staging.trips
    value: 1
  - name: valid_trip_duration
    description: Ensures dropoff is after pickup
    query: |
      SELECT COUNT(*) = 0 
      FROM staging.trips 
      WHERE dropoff_datetime <= pickup_datetime
    value: 1

@bruin */

WITH deduplicated AS (
    SELECT
        -- Create unified datetime columns (yellow uses tpep_, green uses lpep_)
        COALESCE(tpep_pickup_datetime, lpep_pickup_datetime) AS pickup_datetime,
        COALESCE(tpep_dropoff_datetime, lpep_dropoff_datetime) AS dropoff_datetime,
        
        -- Standard columns
        VendorID AS vendor_id,
        passenger_count,
        trip_distance,
        RatecodeID AS rate_code_id,
        store_and_fwd_flag,
        PULocationID AS pickup_location_id,
        DOLocationID AS dropoff_location_id,
        payment_type,
        fare_amount,
        extra,
        mta_tax,
        tip_amount,
        tolls_amount,
        improvement_surcharge,
        total_amount,
        congestion_surcharge,
        airport_fee,
        taxi_type,
        
        -- Deduplication using ROW_NUMBER based on composite key
        ROW_NUMBER() OVER (
            PARTITION BY 
                COALESCE(tpep_pickup_datetime, lpep_pickup_datetime),
                COALESCE(tpep_dropoff_datetime, lpep_dropoff_datetime),
                PULocationID,
                DOLocationID,
                fare_amount,
                taxi_type
            ORDER BY extracted_at DESC
        ) AS row_num
        
    FROM ingestion.trips
    WHERE 
        -- Filter to time window for incremental processing
        COALESCE(tpep_pickup_datetime, lpep_pickup_datetime) >= '{{ start_datetime }}'
        AND COALESCE(tpep_pickup_datetime, lpep_pickup_datetime) < '{{ end_datetime }}'
        -- Filter out invalid records
        AND COALESCE(tpep_pickup_datetime, lpep_pickup_datetime) IS NOT NULL
        AND COALESCE(tpep_dropoff_datetime, lpep_dropoff_datetime) IS NOT NULL
        AND PULocationID IS NOT NULL
        AND DOLocationID IS NOT NULL
)

SELECT
    -- Create a unique trip_id from composite key (BigQuery syntax)
    TO_HEX(MD5(
        CONCAT(
            CAST(pickup_datetime AS STRING), '_',
            CAST(dropoff_datetime AS STRING), '_',
            CAST(pickup_location_id AS STRING), '_',
            CAST(dropoff_location_id AS STRING), '_',
            CAST(fare_amount AS STRING), '_',
            taxi_type
        )
    )) AS trip_id,
    
    d.vendor_id,
    d.pickup_datetime,
    d.dropoff_datetime,
    d.passenger_count,
    d.trip_distance,
    d.rate_code_id,
    d.store_and_fwd_flag,
    d.pickup_location_id,
    d.dropoff_location_id,
    d.payment_type,
    pl.payment_type_name,
    d.fare_amount,
    d.extra,
    d.mta_tax,
    d.tip_amount,
    d.tolls_amount,
    d.improvement_surcharge,
    d.total_amount,
    d.congestion_surcharge,
    d.airport_fee,
    d.taxi_type

FROM deduplicated d
LEFT JOIN ingestion.payment_lookup pl 
    ON d.payment_type = pl.payment_type_id
WHERE d.row_num = 1  -- Keep only the first occurrence of each duplicate group
