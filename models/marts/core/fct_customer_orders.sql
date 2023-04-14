with

-- Import CTEs => moved to staging
-- customers_base as (

--     select * from {{ source('jaffle_shop', 'customers') }}

-- ),




-- Logical CTEs
-- Staging => moved to staging
-- payments as (

--     select 
--         id as payment_id,
--         orderid as order_id,
--         paymentmethod as payment_method,
--         status as payment_status,
--         round(amount/100.0,2) as payment_amount
--     from payments_base


-- ),

orders as (
    select * from {{ ref('stg_jaffle_shop__orders') }}
),

customers as (
    select * from {{ ref('stg_jaffle_shop__customers') }}
),

payments as (
    select * from {{ ref('stg_stripe__payments') }}
),




---marts

customer_order_history as (

    select 
        
        customers.customer_id,
        customers.surname,
        customers.givenname,
        customers.full_name, 

        min(order_date) as first_order_date,

        min(case 
            when orders.order_status not in ('returned','return_pending') 
            then order_date 
        end) as first_non_returned_order_date,

        max(case 
            when orders.order_status not in ('returned','return_pending') 
            then order_date 
        end) as most_recent_non_returned_order_date,

        coalesce(max(user_order_seq),0) as order_count,

        coalesce(count(case 
            when orders.order_status != 'returned' 
            then 1 end),
            0
        ) as non_returned_order_count,

        sum(case 
            when orders.order_status not in ('returned','return_pending') 
            then payment_amount
            else 0 
        end) as total_lifetime_value,

        sum(case 
            when orders.order_status not in ('returned','return_pending') 
            then payment_amount
            else 0 
        end)
        / nullif(count(case 
            when orders.order_status not in ('returned','return_pending') 
            then 1 end),
            0
        ) as avg_non_returned_order_value,

        array_agg(distinct orders.order_id) as order_ids

    from orders

    join customers
    on orders.customer_id = customers.customer_id

    left outer join payments
    on orders.order_id = payments.order_id

    where orders.order_status not in ('pending') and payments.payment_status != 'fail'

    group by customers.customer_id, customers.full_name, customers.givenname, customers.surname

),








-- Final CTEs 
-- marts
final as (

    select 

        orders.order_id as order_id,
        orders.customer_id,
        customers.surname,
        customers.givenname,
        first_order_date,
        order_count,
        total_lifetime_value,
        payment_amount as order_value_dollars,
        orders.order_status as order_status,
        payments.payment_status as payment_status

    from orders

    join customers
    on orders.customer_id = customers.customer_id

    join customer_order_history
    on orders.customer_id = customer_order_history.customer_id

    left outer join payments
    on orders.order_id = payments.order_id

    where payments.payment_status != 'fail'

)






-- Simple Select Statement
select * from final




-- select 
--     orders.id as order_id,
--     orders.user_id as customer_id,
--     last_name as surname,
--     first_name as givenname,
--     first_order_date,
--     order_count,
--     total_lifetime_value,
--     round(amount/100.0,2) as order_value_dollars,
--     orders.status as order_status,
--     payments.status as payment_status
-- from {{ source('jaffle_shop', 'orders') }} as orders

-- join (
--       select 
--         first_name || ' ' || last_name as name, 
--         * 
--       from {{ source('jaffle_shop', 'customers') }}
-- ) customers
-- on orders.user_id = customers.id

-- join (

--     select 
--         customers.id as customer_id,
--         customers.name as full_name,
--         customers.last_name as surname,
--         customers.first_name as givenname,
--         min(order_date) as first_order_date,
--         min(case when a.status NOT IN ('returned','return_pending') then order_date end) as first_non_returned_order_date,
--         max(case when a.status NOT IN ('returned','return_pending') then order_date end) as most_recent_non_returned_order_date,
--         COALESCE(max(user_order_seq),0) as order_count,
--         COALESCE(count(case when a.status != 'returned' then 1 end),0) as non_returned_order_count,
--         sum(case when a.status NOT IN ('returned','return_pending') then ROUND(c.amount/100.0,2) else 0 end) as total_lifetime_value,
--         sum(case when a.status NOT IN ('returned','return_pending') then ROUND(c.amount/100.0,2) else 0 end)/NULLIF(count(case when a.status NOT IN ('returned','return_pending') then 1 end),0) as avg_non_returned_order_value,
--         array_agg(distinct a.id) as order_ids

--     from (
--       select 
--         row_number() over (partition by user_id order by order_date, id) as user_order_seq,
--         *
--       from {{ source('jaffle_shop', 'orders') }}
--     ) a

--     join ( 
--       select 
--         first_name || ' ' || last_name as name, 
--         * 
--       from {{ source('jaffle_shop', 'customers') }}
--     ) b
--     on a.user_id = customers.id

--     left outer join {{ source('stripe', 'payments') }} c
--     on a.id = c.orderid

--     where a.status NOT IN ('pending') and c.status != 'fail'

--     group by customers.id, customers.name, customers.last_name, customers.first_name

-- ) customer_order_history
-- on orders.user_id = customer_order_history.customer_id

-- left outer join {{ source('stripe', 'payments') }} payments
-- on orders.id = payments.orderid

-- where payments.status != 'fail'