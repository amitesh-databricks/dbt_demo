{{ config(materialized='incremental', unique_key='customer_id') }}

SELECT
    customer_id::INT as customer_id
FROM {{ source('bronze', 'customers') }}

{% if is_incremental() %}
  WHERE ingestion_timestamp > (SELECT MAX(ingestion_timestamp) FROM {{ this }})
{% endif %}