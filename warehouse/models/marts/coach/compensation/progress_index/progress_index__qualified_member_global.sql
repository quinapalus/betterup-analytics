{{ config(
    tags=["eu"],
    schema="coach"
) }}

with metadata as (
    select * from {{ ref('progress_index__metadata_global') }}
),

qualified_assessments as (
-- selects for assessment_ids that had at least 1 out of 5 questions answered
-- this could be adjusted if there is a need
    select assessment_id
    from metadata
    group by 1
    having count(assessment_id) >= 4
),

assessment_summary as (
-- this summary is used later to filter out coach/member/modalities based on minimum session number
    select
        coach_member_key
        , {{ dbt_utils.surrogate_key(['coach_member_key', 'modality']) }} as coach_member_modality_key
        , coach_uuid
        , member_uuid
        , modality
        , count(distinct metadata.assessment_id) as assessment_count
    from metadata
    join qualified_assessments
        on metadata.assessment_id = qualified_assessments.assessment_id -- inner join to only include completed assessments
    -- filter for submitted assessments in the last 12 completed months
    where assessment_submitted_at >= DATEADD(MONTH, -12, CURRENT_DATE())
        and assessment_submitted_at <= LAST_DAY(DATEADD(MONTH, -1, CURRENT_DATE()))
        and assessment_submitted_at is not null --filter for only submitted assessments
        and modality not in ('extended network other', 'other')
    group by 1,2,3,4,5
),

session_summary as (
-- this summary is used later to filter out coach/member/modalities based on minimum session number
    select
        coach_member_key
        , {{ dbt_utils.surrogate_key(['coach_member_key', 'modality']) }} as coach_member_modality_key
        , coach_uuid
        , member_uuid
        , modality
        , count(distinct appointment_id) as session_count
    from metadata
    where appointment_completed_at is not null --filter for only completed sessions
    and modality not in ('extended network other', 'other')
    group by 1,2,3,4,5
),

modality_summary as (
    select
        session_summary.*
        , assessment_summary.assessment_count
    from session_summary
    left join assessment_summary
        on session_summary.coach_member_modality_key = assessment_summary.coach_member_modality_key
),

modality_pivot as (
    select
        coach_member_key
        , coach_member_modality_key
        , coach_uuid
        , member_uuid
        , modality
        , array_agg(distinct modality) over (partition by coach_member_key) as modality_array
        , array_agg(distinct case when modality = 'extended network' then true else false end) over (partition by coach_member_key)[0] as had_en
        , sum(case when modality = 'lead' then session_count else 0 end) over (partition by coach_member_key) as lead_session_count
        , sum(case when modality = 'd2c' then session_count else 0 end) over (partition by coach_member_key) as d2c_session_count
        , sum(case when modality = 'care' then session_count else 0 end) over (partition by coach_member_key) as care_session_count
        , sum(case when modality = 'extended network' then session_count else 0 end) over (partition by coach_member_key) as en_session_count
        , sum(case when modality = 'on demand' then session_count else 0 end) over (partition by coach_member_key) as od_session_count
        , sum(case when modality = 'lead' then assessment_count else 0 end) over (partition by coach_member_key) as lead_assessment_count
        , sum(case when modality = 'd2c' then assessment_count else 0 end) over (partition by coach_member_key) as d2c_assessment_count
        , sum(case when modality = 'care' then assessment_count else 0 end) over (partition by coach_member_key) as care_assessment_count
        , sum(case when modality = 'extended network' then assessment_count else 0 end) over (partition by coach_member_key) as en_assessment_count
        , sum(case when modality = 'on demand' then assessment_count else 0 end) over (partition by coach_member_key) as od_assessment_count
    from modality_summary
    group by coach_member_key, coach_member_modality_key, coach_uuid, member_uuid, modality, session_count, assessment_count
),

qualified_member_filter as (
-- filters for coach_member_modality_key based on session and assessment thresholds
    select *
    from modality_pivot
    where (    (array_size(modality_array) = 1 and modality = 'lead' and lead_session_count >= 3 and lead_assessment_count >= 1)
            or (array_size(modality_array) = 1 and modality = 'care' and care_session_count >= 3 and care_assessment_count >= 1)
            or (array_size(modality_array) = 1 and modality = 'd2c' and d2c_session_count >= 3 and d2c_assessment_count >= 1)
            or (array_size(modality_array) = 1 and modality = 'extended network' and en_session_count >= 1 and en_assessment_count >= 1)
            or (array_size(modality_array) = 1 and modality = 'on demand' and od_session_count >= 1 and od_assessment_count >= 1)
            --this logic makes sure that sessions/assessments of the same coach/member combo are being counted
            --across lead/care/d2c modalities but excludes 'extended network' and 'on demand'
            --example: a member switches from lead to d2c but uses the same coach -> this should be count together
            or (array_size(modality_array) > 1 and modality not in ('extended network', 'on demand')
                and (lead_session_count + care_session_count + d2c_session_count) >= 3
                and (lead_assessment_count + care_assessment_count + d2c_assessment_count) >= 1)    )
            --this logic makes sure to count 'extended network' and 'on demand' separately in case a coach/member combo
            --also includes lead/care/d2c modalities between the same coach/member
            or (array_size(modality_array) > 1 and modality = 'extended network' and en_session_count >= 1 and en_assessment_count >= 1)
            or (array_size(modality_array) > 1 and modality = 'on demand' and od_session_count >= 1 and od_assessment_count >= 1)

)

select * from session_summary
