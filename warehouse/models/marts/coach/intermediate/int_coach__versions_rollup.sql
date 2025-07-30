with versions as (

    select * from {{ ref('stg_app_centralized__versions') }}
    where item_type = 'Coach::CoachProfile'

),

base as (
    select
      version_id as id,
      item_type,
      item_uuid as coach_profile_uuid,
      created_at as updated_at,
      obj_changes.path as keys,
      obj_changes.value
    from versions
    join lateral flatten (input => object_changes) as obj_changes
    where event in ('create', 'update')
),

first_staffable_at as (

    select
        coach_profile_uuid,
        min(updated_at) as first_staffable_at
    from base
    where (keys = 'staffable_state' and value[1]::varchar = 'staffable')
    or (keys = 'staffable' and value[1]::varchar = 'true')
    group by 1

),

last_staffable_at as (

    select
        base.coach_profile_uuid,
        max(updated_at) as last_staffable_at
    from base
    left join first_staffable_at as fs
        on base.coach_profile_uuid = fs.coach_profile_uuid

    where
        -- make sure last staffable date is greater than first staffable date
        updated_at > fs.first_staffable_at
        and keys = 'staffable_state'
        and (value[1]::varchar = 'offboarded_voluntary'
        or value[1]::varchar = 'offboarded_involuntary')
    group by 1

)

select
    fs.coach_profile_uuid,
    first_staffable_at,
    last_staffable_at
from first_staffable_at as fs
left join last_staffable_at as ls
    on fs.coach_profile_uuid = ls.coach_profile_uuid