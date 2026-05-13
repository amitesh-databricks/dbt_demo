{% snapshot product_snapshot %}

{{
    config(
        target_schema='snapshots',

        unique_key='product_id',

        strategy='check',

        check_cols='all',

        invalidate_hard_deletes=True,

        file_format='csv',

        tags=['snapshot', 'product_scd2']
    )
}}

SELECT *

FROM {{ ref('_stg_products') }}

{% endsnapshot %}