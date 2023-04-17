
select
    {{ dbt_utils.generate_surrogate_key(['customer_id','order_date']) }} as surrogate_key,
    *
    
from {{ ref('stg_jaffle_shop__orders') }}