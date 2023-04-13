{{
    config(
        materialized='incremental',
        unique_key='unique_key'
    )
}}

with 
source as (
    select * from {{ source('austin_crime', 'austin_crime') }}
)

select
    unique_key as record_id,
    address,
    * except(unique_key, address)
from source