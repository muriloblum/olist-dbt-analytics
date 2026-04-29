with source as (
    select * from {{ source('olist', 'olist_geolocation_dataset') }}
)

, deduplicated as (
    select
        *
        , row_number() over (
            partition by geolocation_zip_code_prefix 
            order by geolocation_zip_code_prefix
        ) as rn
    from source
)

select
    geolocation_zip_code_prefix as zip_code_prefix
    , geolocation_lat as latitude
    , geolocation_lng as longitude
    , geolocation_city as city
    , geolocation_state as state
from deduplicated
where rn = 1