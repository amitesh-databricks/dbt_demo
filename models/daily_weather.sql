with daily_weather as (

    select 
    date(time) as daily_weather,
    weather,
    temp,
    pressure,
    humidity,
    clouds

     from {{ source('demo', 'weather') }}
),
daily_weather_agg as (
    select daily_weather, weather, count(weather) from daily_weather group by daily_weather, weather
)

select * from daily_weather_agg