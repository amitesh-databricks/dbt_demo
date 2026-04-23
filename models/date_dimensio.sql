with cte as (
    select 
    to_timestamp(started_at) AS started_at,
    date(to_timestamp(started_at) ) as date_started_at,

    from
    {{source('demo','bike')}}
    where 
    started_at !='started_at'

)

select
* from cte