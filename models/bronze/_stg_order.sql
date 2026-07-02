{{ config(
    materialized='incremental',
    unique_key='order_id'
) }}

WITH raw_orders AS (
    SELECT
        order_id::string as order_id,
        product_id::string as product_id,
        customer_id::string as customer_id,
        order_date::date as order_date,
        status::string as status,
        -- The math: Price * Quantity * (1 - Discount Percentage)
        (unit_price * quantity * (1 - (discount_pct / 100.0)))::decimal(10,2) as total_order_amount_native,
        currency::string as currency
    FROM {{ source('bronze', 'orders') }}
)

SELECT DISTINCT
    order_id,
    product_id,
    customer_id,
    order_date,
    status,
    total_order_amount_native as amount_eur, -- For consistency with payments
    -- Apply your conversion macro to the CALCULATED TOTAL
    {{ convert_eur_to_usd('total_order_amount_native') }} as amount_usd,
    currency
FROM raw_orders

{% if is_incremental() %}
    WHERE order_date > (SELECT MAX(order_date) FROM {{ this }})
{% endif %}