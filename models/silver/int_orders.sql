{{ config(
    materialized='view',
    alias='int_orders'
) }}

with orders as (
    select * from {{ ref('_stg_order') }}
),

customers as (
    select * from {{ ref('_stg_customers') }}
),

final as (
    select
        o.order_id,
        o.customer_id,
        c.first_name || ' ' || c.last_name as customer_name,
        o.order_date,
        o.status,
        o.amount_usd,
        -- Business logic: flag orders that are finished
        case 
            when o.status = 'completed' then true 
            else false 
        end as is_finalized
    from orders o
    left join customers c 
        on o.customer_id = c.customer_id
)

select * from final