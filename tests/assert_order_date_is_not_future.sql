-- Any rows returned by this query represent a test failure
select
    order_id,
    order_date
from {{ ref('_stg_order') }}
where order_date > current_date()