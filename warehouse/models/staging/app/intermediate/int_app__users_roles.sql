with users_roles as (
    select * from {{ ref('stg_app__users_roles') }}
),

role_changes as (
    select * from {{ ref('stg_app__user_role_changes')}}
),

removed_roles as (
    select user_id,
          role_id,
          event_type,
          row_number() over (partition by role_id, user_id order by created_at desc) as row_num
    from role_changes
    qualify row_num = 1 and event_type = 'removed'
),

final as (

select distinct
  ur.role_id,
  ur.user_id
from users_roles as ur
left join removed_roles as r
  on ur.role_id = r.role_id 
  and ur.user_id  = r.user_id
where r.user_id is null and r.role_id is null -- picking up only records that have not been deleted

)

select
    {{ dbt_utils.surrogate_key(['role_id', 'user_id' ]) }} as primary_key,
    *
from final