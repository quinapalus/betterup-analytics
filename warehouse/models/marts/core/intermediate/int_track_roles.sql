with app_roles as (
  select * from {{ ref('stg_app__roles') }}
),

users_roles as (
  select * from {{ ref('int_app__users_roles') }}
),

user_info as (
  select * from {{ref('stg_app__users')}}
),
joined as (

  select distinct
    resource_id as track_id,
    case
      when name = 'track_admin' then 'partner'
      when name = 'deployment_coordinator' then 'engagement_manager'
      when name = 'deployment_owner' then 'relationship_manager'
    end as role,
    ui.first_name || ' ' || ui.last_name as name
  from app_roles as r
  inner join users_roles as ur
    on r.role_id = ur.role_id
  inner join user_info as ui
    on ur.user_id = ui.user_id
  where r.resource_type = 'Track'

),
final as (

  select {{ dbt_utils.surrogate_key(['track_id','role','name'])}} as int_track_roles_id 
    ,*
  from joined
  
)

select * from final
