with 

source as (
    select * from {{ source('stripe', 'payments') }} 
)



select
    id as payment_id,
    orderid as order_id,
    paymentmethod as payment_method,
    -- amount is stored in cents, convert it to dollarsß
    status as payment_status,
    -- amount / 100 as amount,
    {{ cents_to_dollars('amount', 4) }} as amount,
    created as created_at

from source

-- from raw.payments
-- from {{ source('stripe', 'payments') }}