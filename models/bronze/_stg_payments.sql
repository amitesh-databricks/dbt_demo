{{ config(
    materialized='incremental', 
    unique_key='payment_id'
) }}

-- The 'if' block here ensures we don't even try to query the 
-- max date if the table isn't ready for it yet.
{% if is_incremental() %}
WITH latest_payment AS (
    SELECT MAX(payment_date) as max_date FROM {{ this }}
)
{% endif %}

SELECT
    payment_id::string as payment_id,
    order_id::string as order_id,
    payment_date::date as payment_date,
    amount::decimal(10,2) as amount_eur,
    {{ convert_eur_to_usd('amount') }} as amount_usd,
    currency::string as currency,
    status::string as status
FROM {{ source('bronze', 'payment') }}

{% if is_incremental() %}
    WHERE payment_date > (SELECT max_date FROM latest_payment)
{% endif %}