{{
  config(
    tags=["eu"]
  )
}}

with reporting_group_assignments AS (

  select * from {{ ref('dim_reporting_group_assignments') }}

),

tracks AS (

  select * from {{ ref('dim_tracks') }}

),

member_rollup as (

    select
        rga.member_id,
        ARRAY_AGG(distinct
            case when associated_record_type = 'Track' then rga.associated_record_id
            end) as lifetime_track_ids,
        ARRAY_AGG(distinct t.name) as lifetime_track_names,
        ARRAY_AGG(distinct rga.reporting_group_id) as lifetime_reporting_group_ids
    from reporting_group_assignments rga
    left join tracks t
        on rga.associated_record_id = t.track_id
        and rga.associated_record_type = 'Track'
    group by 1

)

select * from member_rollup