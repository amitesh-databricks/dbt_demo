WITH joined AS (
    SELECT 
        o.order_id,
        c.name as customer_name,
        p.amount_usd,
        o.order_date
    FROM {{ ref('stg_orders') }} o
    JOIN {{ ref('stg_customers') }} c ON o.customer_id = c.customer_id
    JOIN {{ ref('stg_payments') }} p ON o.order_id = p.order_id
    WHERE o.status = 'COMPLETED' -- Filter
)
SELECT 
    *,
    RANK() OVER (PARTITION BY customer_name ORDER BY order_date DESC) as order_rank
FROM joined