{{ config(
    materialized='incremental', 
    unique_key='payment_id',
    alias='stg_payments'
) }}

WITH source_data AS (
    -- Use DISTINCT here to ensure we don't ingest the same payment twice from Bronze
    SELECT DISTINCT
        payment_id::string as payment_id,
        order_id::string as order_id,
        payment_date::date as payment_date,
        amount::decimal(10,2) as amount_eur,
        {{ convert_eur_to_usd('amount') }} as amount_usd,
        currency::string as currency,
        status::string as status
    FROM {{ source('bronze', 'payment') }}

    {% if is_incremental() %}
        WHERE payment_date > (SELECT MAX(payment_date) FROM {{ this }})
    {% endif %}
),

currency_map AS (
    -- Ensure we only get one name per code to prevent join fan-out
    SELECT DISTINCT currency_code, currency_name 
    FROM {{ ref('manual_currency_map') }}
)

SELECT
    s.payment_id,
    s.order_id,
    s.payment_date,
    s.amount_eur,
    s.amount_usd,
    s.currency,
    c.currency_name,
    s.status
FROM source_data s
LEFT JOIN currency_map c
    ON s.currency = c.currency_code