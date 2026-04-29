{{ config(materialized='incremental', unique_key='payment_id') }}

SELECT
    payment_id::INT as payment_id,
    order_id::INT as order_id,
    amount_eur::DECIMAL(10,2) as amount_eur,
    {{ convert_eur_to_usd('amount_eur') }} as amount_usd,
    currency::STRING as currency
FROM {{ source('raw_data', 'payment') }}

{% if is_incremental() %}
  WHERE ingestion_timestamp > (SELECT MAX(ingestion_timestamp) FROM {{ this }})
{% endif %}