{{
  config(
    tags=["eu"]
  )
}}

with roles as (
    select * from {{ ref('stg_app__roles')}}
),

users_roles as (
    select * from {{ ref('int_app__users_roles')}} 
)
select 
    --primary key
    {{ dbt_utils.surrogate_key(['u.user_id','u.role_id'])}} as users_roles_id,

    --foreign keys
    u.user_id,
    u.role_id,
    r.resource_id,
    
    --logical data
    r.name as role,
    r.resource_type

from users_roles as u
left join roles as r
    on u.role_id = r.role_id

