{{
  config(
    tags=["eu"]
  )
}}

with activities as (

  select * from {{ref('stg_app__activities')}}

),

resources AS (

  select * from {{ref('stg_app__resources')}}

),

coach_assignments as (

  select * from {{ref('stg_app__coach_assignments')}}

),

final as (
  
  select
    a.activity_id,
    a.member_id,
    a.creator_id as coach_id,
    a.created_at,
    a.viewed_at, -- populated for activities starting Dec 2016: https://github.com/betterup/betterup-app/issues/3768
    a.viewed_at is not null as is_viewed,
    datediff('d', a.created_at, a.viewed_at) as days_to_view,
    a.completed_at,
    a.completed_at is not null as is_completed,
    datediff('s', a.created_at, a.completed_at) / 86400.0 as days_to_complete,
    a.favorited_at, -- member saved to Bookmarked list
    a.rating, -- rating was re-introduced in Sept 2017
    a.resource_id,
    a.updated_at,
    a.associated_record_id,
    a.associated_record_type,
    r.title as resource_title,
    r.type as resource_type,
    r.content as resource_content,
    r.content_modality as resource_content_modality,
    r.duration as duration_minutes,
    r.duration / 60.0 as duration_hours,
    r.author,
    r.host,
    r.verb as resource_verb,
    ca.role as coach_type
  from activities as a
  left join resources as r
    on a.resource_id = r.resource_id
  left join coach_assignments as ca
    on a.creator_id = ca.coach_id
    and a.member_id = ca.member_id
    and a.created_at >= ca.created_at
    and a.created_at < ca.ended_at
  qualify row_number() over (
    partition by a.activity_id, a.member_id, a.creator_id 
    order by case ca.role when 'primary' then 1 when 'secondary' then 2 else 99 end,
      ca.created_at
) = 1 /* In the event a member has more than one assignment to the same coach in 
          the same timeframe pick just one to keep activity_id unique
          and in the case of differing roles, prefer primary, then secondary, then
          anything else */

)
 
select *
from final

