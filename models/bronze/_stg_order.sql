{{ config(materialized='incremental', unique_key='payment_id') }}

SELECT
    payment_id::string as payment_id,
    order_id::string as order_id,
    unit_price::DECIMAL(10,2) as amount_eur,
    order_date::Date as order_date,
    status:: string as status,
    customer_id::string as customer_id,
    {{ convert_eur_to_usd('unit_price') }} as amount_usd,
    currency::STRING as currency
FROM {{ source('bronze', 'orders') }}

-- {% if is_incremental() %}
--  WHERE orders.order_date > (SELECT MAX(orders.order_date) FROM {{ this }})
-- {% endif %}