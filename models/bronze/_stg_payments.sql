{{ config(
    materialized='incremental', 
    unique_key='payment_id',
    alias='stg_payments'
) }}

WITH source_data AS (
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
        -- Only pull records newer than the max date already in this table
        WHERE payment_date > (SELECT MAX(payment_date) FROM {{ this }})
    {% endif %}
),

currency_map AS (
    SELECT * FROM {{ ref('manual_currency_map') }}
)

SELECT
    s.payment_id,
    s.order_id,
    s.payment_date,
    s.amount_eur,
    s.amount_usd,
    s.currency,
    -- Bringing in the full name from our seed file
    c.currency_name,
    s.status
FROM source_data s
LEFT JOIN currency_map c
    ON s.currency = c.currency_code