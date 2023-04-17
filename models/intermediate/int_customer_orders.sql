with

    orders as (
        select * from {{ ref('int_orders') }}
    ), 

    customers as (
        select * from {{ ref('stg_jaffle_shop__customers') }}
    ),   

    -- customer level 
    customer_orders as (
        select 
            customers.customer_id,
            min(ORDER_DATE) as first_order_date,
            max(ORDER_DATE) as most_recent_order_date,
            count(order_id) AS number_of_orders,
            max(givenname) as customer_first_name,
            max(surname) as customer_last_name
        from orders left join customers
            on customers.customer_id = orders.customer_id
        group by 1           
    )


select * from customer_orders