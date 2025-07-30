{{ config(
    tags=["eu"],
    schema="coach"
) }}

with qualified_member_filter as (
    select * from {{ ref('progress_index__qualified_member_global') }}
),

qualified_coach_filter as (
-- filters for coaches that had at least 3 distinct qualified members rate their sessions
    select
        coach_uuid
        , count(distinct member_uuid) as member_count
    from qualified_member_filter
    group by 1
    having count(distinct member_uuid) >= 3
)

select * from qualified_coach_filter
