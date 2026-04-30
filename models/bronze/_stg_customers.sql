{{ config(materialized='view') }}

SELECT
    customer_id,
    first_name,  -- Ensure this is here
    last_name,   -- Ensure this is here
    email,
    city,
    country,
    customer_segment,
    is_active
FROM {{ source('bronze', 'customers') }}

-- {% if is_incremental() %}
--  WHERE ingestion_timestamp > (SELECT MAX(ingestion_timestamp) FROM {{ this }})
-- {% endif %}