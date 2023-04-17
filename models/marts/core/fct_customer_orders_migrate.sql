with

    orders as (select * from {{ ref("int_orders") }}),

    customer_orders as (select * from {{ ref("int_customer_orders") }}),

    -- - logic 
    -- order level info 
    completed_orders as (
        select
            order_id,
            customer_id,
            payment_finalized_date,
            order_status,
            order_value_dollars as total_amount_paid,
            -- ROW_NUMBER() OVER (ORDER BY order_id) as transaction_seq,
            order_date as order_placed_at,
            row_number() over (order by order_date, order_id) as transaction_seq,
            row_number() over (
                partition by customer_id order by order_date, order_id
            ) as customer_sales_seq,
            sum(order_value_dollars) over (
                partition by customer_id order by order_id
            ) as customer_lifetime_value
        from orders
        -- where order_status = "completed"
    ),

    order_customers as (
        select
            completed_orders.order_id,
            completed_orders.customer_id,
            completed_orders.order_placed_at,
            completed_orders.order_status,
            completed_orders.total_amount_paid,
            completed_orders.payment_finalized_date,
            customer_orders.customer_first_name,
            customer_orders.customer_last_name,
            completed_orders.transaction_seq,
            completed_orders.customer_sales_seq,
            -- query i need to learn
            -- case
            --     when
            --         (
            --             rank() over (
            --                 partition by customer_orders.customer_id
            --                 order by order_placed_at, order_id
            --             )
            --             = 1
            --         )
            --     then 'new'
            --     else 'return'
            -- end as nvsr,

            customer_lifetime_value,
            customer_orders.first_order_date as fdos

        from completed_orders
        left join
            customer_orders
            on completed_orders.customer_id = customer_orders.customer_id
    ),

    final as (
        select 
            order_id,
            customer_id,
            order_placed_at,
            order_status,
            total_amount_paid,
            payment_finalized_date,
            customer_first_name,
            customer_last_name,
            transaction_seq,
            customer_sales_seq,
            case
                when customer_sales_seq = 1 
                    then 'new'
                    else 'return'
                end as nvsr,
            customer_lifetime_value,
            fdos
        from order_customers
                         
    )

select *
from final
