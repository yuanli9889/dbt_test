with

customers as (
    select * from {{ ref('stg_jaffle_shop__customers') }}
),

orders as (
    select * from {{ ref('int_orders') }} --imported an intermedia model
),

payments as (
    select * from {{ ref('stg_stripe__payments') }}
),

customer_orders as (

    select 
        orders.*,
        -- customers.customer_id,
        customers.surname,
        customers.givenname,
        customers.full_name, 

        --customer level aggregations
        min(orders.order_date) over (
            partition by orders.customer_id
        )as first_order_date,

        min(orders.valid_order_date) over (
            partition by orders.customer_id
        ) as first_non_returned_order_date,

        max(orders.valid_order_date)  over (
            partition by orders.customer_id
        ) as most_recent_non_returned_order_date,

        count(*)  over (
            partition by orders.customer_id
        ) as order_count,
        -- coalesce(max(user_order_seq),0)  over (
        --     partition by orders.customer_id
        -- ) as order_count,

        -- sum(nvl2(orders.valid_order_date, 1, 0)) over(
        --     partition by orders.customer_id
        -- ) as customer_non_returned_order_count,

        coalesce(count(case 
            when orders.valid_order_date is not null
            then 1 end) over (
            partition by orders.customer_id
        ) ,
            0
        )  as non_returned_order_count,        

        sum(case 
            when orders.valid_order_date is not null
            then orders.order_value_dollars
            else 0 
        end) over (
            partition by orders.customer_id
        ) as total_lifetime_value,        

        array_agg(orders.order_id) over(
            partition by orders.customer_id
            ) as order_ids 

    from orders 
    join customers
    on orders.customer_id = customers.customer_id

),

avg_order_values as (
    select 
        *,
        total_lifetime_value / non_returned_order_count as avg_non_returned_order_value
    from customer_orders
),


final as (

    select 

        order_id,
        customer_id,
        surname,
        givenname,
        first_order_date,
        order_count,
        total_lifetime_value,
        order_value_dollars,
        order_status,
        payment_status

    from avg_order_values
)


-- Simple Select Statement
select * from final




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

-- orders as (
--     select * from {{ ref('stg_jaffle_shop__orders') }}
-- ),

-- payments as (
--     select * from {{ ref('stg_stripe__payments') }}
--     where payment_status != 'fail' -- example of centralising logic, instead of having two where below
-- ),



-- furthur refactoring after CTE grouping and moving CTE to stg, putting order_values_joined to intermedia => int_ , along with above import CTE

-- order_totals as (
--     select 
--         order_id,
--         payment_status,
--         sum(payment_amount) as order_value_dollars
--     from payments
--     group by 1,2

-- ),

-- order_values_joined as (
--     select 
--         orders.*,
--         order_totals.payment_status,
--         order_totals.order_value_dollars
--     from orders 
--     left join order_totals
--         on orders.order_id = order_totals.order_id
-- ),



---marts

-- this is an aggregating CTE to order level, can be treat as intermediate model 
-- customer_order_history as (

--     select 
        
--         customers.customer_id,
--         customers.surname,
--         customers.givenname,
--         customers.full_name, 
--         min(orders.order_date) as first_order_date,

-- -- further refactoring: move messy code to stg
--         -- min(case 
--         --     when orders.order_status not in ('returned','return_pending') 
--         --     then order_date 
--         -- end) as first_non_returned_order_date,
--         min(orders.valid_order_date) as first_non_returned_order_date,

--         max(orders.valid_order_date) as most_recent_non_returned_order_date,

--         coalesce(max(user_order_seq),0) as order_count,

-- --use same logic 
--         -- coalesce(count(case 
--         --     when orders.order_status != 'returned' 
--         --     then 1 end),
--         --     0
--         -- ) as non_returned_order_count,
--         coalesce(count(case 
--             when orders.valid_order_date is not null
--             then 1 end),
--             0
--         ) as non_returned_order_count,        

--         -- sum(case 
--         --     when orders.order_status not in ('returned','return_pending') 
--         --     then payment_amount
--         --     else 0 
--         -- end) as total_lifetime_value,
--         sum(case 
--             when orders.valid_order_date is not null
--             then orders.order_value_dollars
--             else 0 
--         end) as total_lifetime_value,        

--         sum(case 
--             when orders.valid_order_date is not null
--             then orders.order_value_dollars
--             else 0 
--         end)
--         / nullif(count(case 
--             when orders.valid_order_date is not null
--             then 1 end),
--             0
--         ) as avg_non_returned_order_value,

--         array_agg(distinct orders.order_id) as order_ids

--     from orders 

--     join customers
--     on orders.customer_id = customers.customer_id

--     -- left outer join payments
--     -- on order_values_joined.order_id = payments.order_id

--     -- group by customers.customer_id, customers.full_name, customers.givenname, customers.surname

-- ),








-- Final CTEs 
-- marts
-- final as (

--     select 

--         orders.order_id as order_id,
--         orders.customer_id,
--         customers.surname,
--         customers.givenname,
--         first_order_date,
--         order_count,
--         total_lifetime_value,
--         payment_amount as order_value_dollars,
--         orders.order_status as order_status,
--         payments.payment_status as payment_status

--     from orders

--     join customers
--     on orders.customer_id = customers.customer_id

--     join customer_order_history
--     on orders.customer_id = customer_order_history.customer_id

--     left outer join payments
--     on orders.order_id = payments.order_id

--     -- where payments.payment_status != 'fail'

-- )






-- -- Simple Select Statement
-- select * from final




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