{% snapshot orders_snapshot %}

{{
    config(
      target_schema='DBT_PROJECT_SCHEMA',
      unique_key='order_id',
      strategy='check',
      check_cols=['status', 'order_date'],
    )
}}

select * from {{ ref('_stg_order') }}

{% endsnapshot %}