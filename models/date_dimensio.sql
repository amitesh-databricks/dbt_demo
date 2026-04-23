with cte as (
    select 
    to_timestamp(started_at) AS started_at,
    date(to_timestamp(started_at) ) as date_started_at,
    hour(to_timestamp(started_at)) as hour_started_at,
    {{day_type('started_at')}} as DAY_TYPE,

    {{get_session('started_at')}} as Station_of_year,
    {{ calc_trip_duration_minutes('started_at', 'ended_at') }} AS trip_duration_minutes,
    {{ extract_time_features('started_at', prefix='start_') }} AS Extracted_time,
    {{ normalize_rideable_type('rideable_type') }} AS bike_type

    from
    {{source('demo','bike')}}
    where 
    started_at !='started_at' or ended_at !='ended_at'

)

select
* from cte