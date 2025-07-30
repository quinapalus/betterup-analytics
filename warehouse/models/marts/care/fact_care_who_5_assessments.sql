WITH scores AS (

  SELECT * FROM  {{ ref('stg_app__scores') }}

),

assessments AS (

  SELECT * FROM  {{ ref('fact_assessments') }}

),

appointments AS (

  SELECT * FROM  {{ ref('stg_app__appointments') }}

),

member_platform_calendar as (

    select * from analytics.analytics.member_platform_calendar

),

first_session as (

    select distinct
        a.member_id,
        first_value(a.started_at) over (partition by a.member_id order by a.started_at)  as first_session_started_at
    from appointments a

),

total_sessions as (

    select
          a.MEMBER_ID,
          count(distinct a.started_at) as session_count
      from
          appointments a
      group by 1
),


assessment_classification as (

      select distinct
        a.user_id,
        a.created_at as assessment_at,
        ts.session_count,
        fs.first_session_started_at as first_session_before_assessments,
        fs2.first_session_started_at as first_session_overall,
        last_value(a.created_at)
          over (partition by a.user_id, fs.first_session_started_at order by a.created_at) as last_assessment_before_session,
        first_value(a.created_at)
          over (partition by a.user_id, fs2.first_session_started_at order by a.created_at) as first_assessment,
        case
            when coalesce(ts.session_count,0) = 0 and first_assessment = a.created_at -- no prior sessions, first assessment is baseline
                then 1
            when first_assessment > first_session_overall -- (update:BUAPP-68589) and first_assessment = a.created_at -- first WHO-5 assessment post prior session (not baseline as user has previous sessions)
                then 0
            when a.created_at = last_assessment_before_session and fs.first_session_started_at is not null -- last assessment before first session in case of prior assessments
                then 1
            else 0
        end as is_assessment_baseline,
        a.assessment_id
      from scores s
      join assessments a on s.assessment_id = a.assessment_id
        left join first_session fs on a.user_id = fs.member_id and a.created_at < fs.first_session_started_at
        left join first_session fs2 on a.user_id = fs2.member_id
        left join total_sessions ts on a.user_id = ts.member_id
      where s.key = 'who_5_overall'

),

final as (

      select distinct
          s.score_id,
          s.assessment_id,
          a.user_id as member_id,
          s.key as scores_key,
          s.type as scores_type,
          s.raw_score,
          s.scale_score,
          ac.is_assessment_baseline,
          case
            when ac2.assessment_at is not null
                then 1
            else 0
          end as post_baseline_assessment,
          case
            when a.associated_record_type = 'Appointment'
                then 1
            else 0
          end as associated_with_session,
          case
            when post_baseline_assessment = 1
                then dense_rank() over (partition by a.user_id, post_baseline_assessment, s.key order by a.created_at)
            else null
          end as sequence_from_baseline,
          count(distinct case when post_baseline_assessment = 1 then a.assessment_id end) over (partition by a.user_id) as assessment_cnt_from_baseline,
          dense_rank() over (partition by a.user_id, s.key order by a.created_at) as sequence_overall ,
          count(distinct a.assessment_id) over (partition by a.user_id) as assessment_cnt_overall,
          s.created_at as score_recorded_at,
          a.created_at as assessment_date,
          ceil(ceil(datediff('day', mpc.first_care_access_date, ac.first_assessment))/30) + 1 as month_of_access
      from scores as s
      join assessments as a on s.assessment_id = a.assessment_id
      join assessment_classification ac on a.assessment_id = ac.assessment_id
      join member_platform_calendar mpc on mpc.member_id = a.user_id
      left join assessment_classification ac2 on a.user_id = ac2.user_id and a.created_at >= ac2.assessment_at and ac2.is_assessment_baseline = 1
      where s.key like 'who_5_%'
)

select * from final
