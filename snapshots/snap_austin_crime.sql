{% snapshot austin_crime_snapshot %}

{% set new_schema = target.schema + '_snapshot' %}

    {{
        config(
            target_schema=new_schema,
            unique_key='unique_key',
            
            strategy='timestamp',
            updated_at='timestamp'
        )
    }}

    select * from {{ source('austin_crime', 'austin_crime') }}

 {% endsnapshot %}