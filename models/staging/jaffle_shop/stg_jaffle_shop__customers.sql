with

source as (
    select * from {{ source('jaffle_shop', 'customers') }}

),

transformed as ( 

    select 
        id as customer_id,
        last_name as surname,
        first_name as givenname,
        first_name || ' ' || last_name as full_name

    from source

)

select * from transformed



-- with 
-- -- refactor staging models based on configured source, src_jaffle_shop.yml
-- source as (
--     select * from {{ source('jaffle_shop', 'customers') }}
-- )

-- select
--     id as customer_id,
--     first_name,
--     last_name
-- from source




