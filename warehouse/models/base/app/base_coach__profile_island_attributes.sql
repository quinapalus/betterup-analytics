with source as (
    select * from {{ source('coach', 'profile_island_attributes') }}
)

select *
from source
where {{ filter_by_island(var("account_env", "")) }}  -- macro called filter_by_island.sql
-- this table is being filtered by 'island' depending on which instance this data model is running in