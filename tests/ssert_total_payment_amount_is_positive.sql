select
    customer_id, 
    amount
from {{ ref('orders') }}
where amount < 0