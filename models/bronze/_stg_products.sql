{{ config(
    materialized='incremental', 
    unique_key='product_id'
) }}

-- The 'if' block here ensures we don't even try to query the 
-- max date if the table isn't ready for it yet.
{% if is_incremental() %}
WITH latest_created_date AS (
    SELECT MAX(created_date) as max_date FROM {{ this }}
)
{% endif %}

SELECT
    product_id::string as product_id,
    SUPPLIER_ID::string as SUPPLIER_ID,
    created_date::date as created_date,
    cost_price::decimal(10,2) as amount_eur,
    list_price::decimal(10,2) as list_price_eur,
    {{ convert_eur_to_usd('cost_price') }} as amount_usd,
    {{ convert_eur_to_usd('list_price') }} as list_price_usd,
    currency::string as currency
FROM {{ source('bronze', 'products') }}

{% if is_incremental() %}
    WHERE created_date > (SELECT max_date FROM latest_created_date)
{% endif %}