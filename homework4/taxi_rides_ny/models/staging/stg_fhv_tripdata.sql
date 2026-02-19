with source as (
    select * from {{ source('raw', 'fhv_tripdata') }}
),

renamed as (
    select
        -- identifiers
        dispatching_base_num,
        cast(request_datetime as timestamp) as pickup_datetime,
        cast(dropoff_datetime as timestamp) as dropoff_datetime,
        cast(pickup_location_id as integer) as pickup_location_id,
        cast(dropoff_location_id as integer) as dropoff_location_id,

        -- trip info
        cast(pickup_census_tract as string) as pickup_census_tract,
        cast(dropoff_census_tract as string) as dropoff_census_tract,
        cast(pickup_community_area as integer) as pickup_community_area,
        cast(dropoff_community_area as integer) as dropoff_community_area,

        -- additional fields as needed
        cast(trip_miles as numeric) as trip_distance,
        cast(trip_seconds as integer) as trip_duration_seconds,
        cast(fare as numeric) as fare_amount,
        cast(tip as numeric) as tip_amount,
        cast(tolls as numeric) as tolls_amount,
        cast(extras as numeric) as extra,
        cast(payment_type as integer) as payment_type

    from source
    where dispatching_base_num is not null
)

select * from renamed