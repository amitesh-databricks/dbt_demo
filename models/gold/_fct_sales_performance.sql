{{ config(
    materialized='table',
    alias='fct_sales_performance'
) }}

with orders as (
    select * from {{ ref('int_orders') }}
),

final as (
    select
        -- Dimensions
        order_date,
        customer_id,
        customer_name,
        
        -- Measures (Aggregations)
        count(order_id) as total_orders,
        sum(amount_usd) as total_revenue_usd,
        round(avg(amount_usd), 2) as avg_order_value_usd,
        
        -- Status Flag (from Silver logic)
        max(case when is_finalized then 1 else 0 end) as has_completed_orders
        
    from orders
    group by 1, 2, 3
)

select * from final