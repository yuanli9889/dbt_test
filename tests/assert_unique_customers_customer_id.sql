select
    id
from {{ ref('orders') }}
group by 1
having count(id) != 1