select
    order_id
from {{ ref('stg_orders') }}
group by 1
having count(order_id) != 1