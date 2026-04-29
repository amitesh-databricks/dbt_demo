with weather_bike_corelation as (
    select 
    t.*,
    w.*
    from {{ ref('trips_fact') }} t
    left join {{ ref('daily_weather') }} w
    on t.TRIP_DATE=w.daily_weather
  order by TRIP_DATE desc
)
select * from weather_bike_corelation