{{ config(
    tags=["eu"],
    schema="coach"
) }}

with metadata as (
    select * from {{ ref('progress_index__metadata_global') }}
),
users as (
    select * from {{ ref('int_app__users') }}
),
qualified_member_filter as (
    select * from {{ ref('progress_index__qualified_member_global') }}
),
qualified_coach_filter as (
    select * from {{ ref('progress_index__qualified_coach_global') }}
),

session_composite_scores as (
-- average of assessment scores by coach/member/session/modality
    select
        metadata.coach_member_key
        , metadata.coach_uuid
        , metadata.member_uuid
        , metadata.appointment_id
        , metadata.modality
        , round(avg(metadata.assessment_score),4) as avg_session_composite_score
    from metadata
        join qualified_member_filter
            --filter for qualified members who meet required session and assessment minimums
            on qualified_member_filter.coach_member_key = metadata.coach_member_key
            and qualified_member_filter.modality = metadata.modality
    --filter to only include assessments that were submitted in the past 12 completed months
    where assessment_submitted_at >= DATEADD(MONTH, -12, CURRENT_DATE())
        and assessment_submitted_at <= LAST_DAY(DATEADD(MONTH, -1, CURRENT_DATE()))
    group by 1,2,3,4,5
),

member_composite_scores as (
-- average assessment scores by coach/member
    select
        coach_member_key
        , coach_uuid
        , member_uuid
        , round(avg(avg_session_composite_score),4) as avg_member_composite_score
    from session_composite_scores
    group by 1,2,3
),

geo_corrected as (
    select
        member_composite_scores.coach_member_key
        , member_composite_scores.coach_uuid
        , member_composite_scores.member_uuid
        , case when users.geo = 'EMEA' then avg_member_composite_score + 0.05
               when users.geo = 'APAC' then avg_member_composite_score + 0.12
               else avg_member_composite_score
          end as geo_corrected_member_composite_score
    from member_composite_scores
    left join users
        on users.user_uuid = member_composite_scores.member_uuid
),

coach_composite_scores as (
    select
        geo_corrected.coach_uuid
        , round(avg(geo_corrected_member_composite_score),4) as avg_coach_composite_scores
    from geo_corrected
    -- filter for coaches that had at least 3 qualified members
    join qualified_coach_filter
        on qualified_coach_filter.coach_uuid = geo_corrected.coach_uuid
    group by 1
),

rescaled_scores as (
    select
        coach_uuid
        , case
               --very low rated coaches can have a rescaled score below 0, we want to cap at 0
               when round(100*((avg_coach_composite_scores - 3.8)/1.2),2) <= 0 then 1
               --coaches with high scores and APAC/EMEA geo adjustment can get a rescaled score over 100, we want to cap at 100
               when round(100*((avg_coach_composite_scores - 3.8)/1.2),2) > 100 then 100
               else round(100*((avg_coach_composite_scores - 3.8)/1.2),2)
          end as rescaled_coach_composite_score
    from coach_composite_scores
)

select * from rescaled_scores
