{% snapshot customer_snapshot %}

{{
    config(
        target_schema='snapshots',

        unique_key='customer_id',

        strategy='check',

        check_cols='all',

        invalidate_hard_deletes=True,

        file_format='delta',

        tags=['snapshot', 'customer_scd2']
    )
}}

SELECT *

FROM {{ ref('_stg_customers') }}

{% endsnapshot %}