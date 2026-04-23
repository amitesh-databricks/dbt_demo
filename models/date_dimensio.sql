with cte as (
    select 
    to_timestamp(started_at) AS started_at,
    date(to_timestamp(started_at) ) as date_started_at,
    hour(to_timestamp(started_at)) as hour_started_at,
    case 
    when dayname(to_timestamp(started_at)) in ('sat','sun')
    then 'weekend'
    else 'businessday'
    end as DAY_TYPE,

    {{get_session('started_at')}} as Station_of_year


    from
    {{source('demo','bike')}}
    where 
    started_at !='started_at'

)

select
* from cte