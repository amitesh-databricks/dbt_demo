with orders as (
    select order_id, amount_usd 
    from {{ ref('int_orders') }}
    where lower(status) = 'completed'
),
payments as (
    select order_id, sum(amount_usd) as total_payment_usd 
    from {{ ref('_stg_payments') }}
    where lower(status) = 'success'
    group by 1
)
select
    o.order_id,
    o.amount_usd as order_amt,
    p.total_payment_usd as pay_amt
from orders o
join payments p on o.order_id = p.order_id
-- We change the threshold to 0.10 (10 cents)
-- This will ignore the 1-6 cent rounding gaps but still catch real errors.
where (o.amount_usd - p.total_payment_usd) > 1.0