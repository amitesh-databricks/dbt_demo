{{ config(materialized='view', schema='silver') }}

WITH joined AS (
    SELECT 
        o.order_id,
        -- Concatenating the names passed from _stg_customers
        c.first_name || ' ' || c.last_name AS customer_name,
        p.amount_usd, 
        o.order_date
    FROM {{ ref('_stg_order') }} AS o
    INNER JOIN {{ ref('_stg_customers') }} AS c 
        ON o.customer_id = c.customer_id
    INNER JOIN {{ ref('_stg_payments') }} AS p 
        ON o.order_id = p.order_id
    WHERE o.status = 'COMPLETED'
)

SELECT 
    order_id,
    customer_name,
    amount_usd,
    order_date,
    RANK() OVER (
        PARTITION BY customer_name 
        ORDER BY order_date DESC, order_id DESC
    ) AS order_rank
FROM joined