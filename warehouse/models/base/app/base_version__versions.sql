with source as (
    select * from {{ source('version', 'versions') }}
)

select *
from source
where {{ filter_by_island(var("account_env", "")) }}  -- macro called filter_by_island.sql