with bike as (

select 
distinct 
start_station_id,
start_station_name,
start_latitude,
start_longitude
from {{ source('demo', 'bike') }}
where ride_id != 'ride_id'

)

select * from bike

