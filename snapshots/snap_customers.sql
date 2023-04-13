{% snapshot customers_snapshot %}

{% set new_schema = target.schema + '_snapshot' %}

    {{
        config(
            target_schema=new_schema,
            unique_key='id',
            
            strategy='check',
            check_cols=['customer_id', 'first_name','last_name']
        )
    }}

    select * from {{ source('jaffle_shop', 'customers') }}

 {% endsnapshot %}