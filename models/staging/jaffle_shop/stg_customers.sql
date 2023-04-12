with 
-- refactor staging models based on configured source, src_jaffle_shop.yml
source as (
    select * from {{ source('jaffle_shop', 'customers') }}
)

select
    id as customer_id,
    first_name,
    last_name
from source