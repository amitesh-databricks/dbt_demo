{{ config(materialized='incremental', unique_key='payment_id') }}

SELECT
    payment_id::string as payment_id,
    order_id::string as order_id,
    amount::DECIMAL(10,2) as amount_eur,
    {{ convert_eur_to_usd('amount') }} as amount_usd,
    currency::STRING as currency
FROM {{ source('bronze', 'payment') }}

-- {% if is_incremental() %}
--  WHERE payment.payment_date > (SELECT MAX(payment.payment_date) FROM {{ this }})
-- {% endif %}