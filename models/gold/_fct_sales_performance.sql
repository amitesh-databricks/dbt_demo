SELECT
    customer_name,
    COUNT(order_id) as total_orders,
    SUM(amount_usd) as lifetime_value
FROM {{ ref('int_order_details') }}
GROUP BY 1