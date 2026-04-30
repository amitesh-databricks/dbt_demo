{{ config(materialized='incremental', unique_key='customer_id') }}

SELECT
    customer_id,
    first_name,  -- Ensure this is here
    last_name,   -- Ensure this is here
    email,
    city,
    country,
    customer_segment,
    is_active,
    signup_date
FROM {{ source('bronze', 'customers') }}

{% if is_incremental() %}
    WHERE signup_date > (SELECT MAX(signup_date) FROM {{ this }})
{% endif %}