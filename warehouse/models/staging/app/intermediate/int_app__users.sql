{{
  config(
    tags=["eu"]
  )
}}


with users as (
  select * from {{ ref('stg_app__users') }}
),

time_zone_geo_categories as (
  select * from {{ ref('int_csv__time_zone_geo_categories') }}
),

roles as (
  select * from {{ ref('stg_app__roles') }}
),
users_roles as (
  select * from {{ ref('stg_app__users_roles') }}
),

global_roles as (
  -- rationalize global roles into array for each user
  select
    u.user_id,
    array_agg(r.name) as roles
  from users_roles as u
  inner join roles as r 
    on u.role_id = r.role_id and  r.resource_type is null
  group by u.user_id

),

final as (

  select
    u.*,
    r.roles,
    coalesce(u.last_engaged_at, u.last_sign_in_at, u.confirmed_at) as last_usage_at,
    tzg.tz_iana,
    tzg.country_code,
    tzg.country_name,
    tzg.subregion_m49,
    tzg.geo
  from users as u
  left join global_roles as r 
    on u.user_id = r.user_id
  left join time_zone_geo_categories as tzg
    on u.time_zone = tzg.time_zone

)

select *
from final