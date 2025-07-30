with track_assignments as (

    select * from {{ ref('stg_app__track_assignments') }}

),


assessments as (

    select * from {{ ref('stg_app__assessments') }}

),


has_baseline AS (

    select distinct track_assignments.member_id
    from track_assignments
    inner join assessments as adaptability
        on adaptability.user_id = track_assignments.member_id
        and adaptability.type = 'Assessments::CareOnboardingEmotionalRegulationAssessment'
        and adaptability.submitted_at is not null
    inner join assessments as outlook
        on outlook.user_id = track_assignments.member_id
        and outlook.type = 'Assessments::CareOnboardingOptimismAssessment'
        and outlook.submitted_at is not null
    inner join assessments as fulfillment
        on fulfillment.user_id = track_assignments.member_id
        and fulfillment.type = 'Assessments::CareOnboardingPurposeAssessment'
        and fulfillment.submitted_at is not null
    inner join assessments as coping
        on coping.user_id = track_assignments.member_id
        and coping.type = 'Assessments::CareOnboardingResilienceAssessment'
        and coping.submitted_at is not null
    inner join assessments as self_care
        on self_care.user_id = track_assignments.member_id
        and self_care.type = 'Assessments::CareOnboardingSelfAwarenessAssessment'
        and self_care.submitted_at is not null

),


most_recent_progress as (

    select assessments.user_id, max (assessments.submitted_at) as last_progress_at
    from assessments
    where assessments.type = 'Assessments::CareWellbeingCheckinAssessment'
        and submitted_at is not null
    group by assessments.user_id

),

baseline_section_scores as (

  -- baseline section scores (stored in separate assessments)
    select
        assessments.user_id,
        assessments.assessment_id,
        case assessments.type
          when 'Assessments::CareOnboardingEmotionalRegulationAssessment' then 'Adaptability'
          when 'Assessments::CareOnboardingOptimismAssessment' then 'Outlook'
          when 'Assessments::CareOnboardingPurposeAssessment' then 'Fulfillment'
          when 'Assessments::CareOnboardingResilienceAssessment' then 'Coping'
          when 'Assessments::CareOnboardingSelfAwarenessAssessment' then 'Self-care'
        end as construct,
        assessments.responses:score:: float as scale_score,
        submitted_at as measurement_date,
        true as is_baseline,
        false as is_progress
    from assessments
    where assessments.type in (
        'Assessments::CareOnboardingEmotionalRegulationAssessment',
        'Assessments::CareOnboardingOptimismAssessment',
        'Assessments::CareOnboardingPurposeAssessment',
        'Assessments::CareOnboardingResilienceAssessment',
        'Assessments::CareOnboardingSelfAwarenessAssessment'
        )
        and assessments.submitted_at is not null
    --Enforce assumption of one onboarding assessment per member, only take the one submitted
    qualify row_number() over (partition by user_id, type order by submitted_at) = 1

)

, adaptability as (

    select * from assessments
    where type = 'Assessments::CareOnboardingEmotionalRegulationAssessment'
        and submitted_at is not null
    qualify row_number() over (partition by user_id order by submitted_at) = 1

)

, outlook as (

    select * from assessments
    where type = 'Assessments::CareOnboardingOptimismAssessment'
        and submitted_at is not null
    qualify row_number() over (partition by user_id order by submitted_at) = 1

)

, fulfillment as (

    select * from assessments
    where type = 'Assessments::CareOnboardingPurposeAssessment'
        and submitted_at is not null
    qualify row_number() over (partition by user_id order by submitted_at) = 1

)

, coping as (

    select * from assessments
    where type = 'Assessments::CareOnboardingResilienceAssessment'
        and submitted_at is not null
    qualify row_number() over (partition by user_id order by submitted_at) = 1

)

, self_care as (

    select * from assessments
    where type = 'Assessments::CareOnboardingSelfAwarenessAssessment'
        and submitted_at is not null
    qualify row_number() over (partition by user_id order by submitted_at) = 1

)

, baseline_overall_wellbeing_scores as (

    -- baseline overall well-being score (average of separate assessments)
    select
        track_assignments.member_id as user_id,
        adaptability.assessment_id,
        'Well-being' as construct,
        (adaptability.responses:score:: float + outlook.responses:score:: float + fulfillment.responses:score:: float + coping.responses:score:: float + self_care.responses:score:: float) / 5.0 as scale_score,
        greatest(adaptability.submitted_at, outlook.submitted_at, fulfillment.submitted_at, coping.submitted_at, self_care.submitted_at) as measurement_date,
        true as is_baseline,
        false as is_progress
    from track_assignments
    inner join adaptability on adaptability.user_id = track_assignments.member_id
    inner join outlook on outlook.user_id = track_assignments.member_id
    inner join fulfillment on fulfillment.user_id = track_assignments.member_id
    inner join coping on coping.user_id = track_assignments.member_id
    inner join self_care on self_care.user_id = track_assignments.member_id

)

, scores as (

    select * from baseline_section_scores

    union all

    select * from baseline_overall_wellbeing_scores

    union all

    -- progress well-being scores (stored in one combined assessment)
    select
        assessments.user_id,
        assessments.assessment_id,
        case r.path
            when 'emotional_regulation_score' then 'Adaptability'
            when 'optimism_score' then 'Outlook'
            when 'purpose_score' then 'Fulfillment'
            when 'resilience_score' then 'Coping'
            when 'self_awareness_score' then 'Self-care'
            when 'score' then 'Well-being'
        end as construct,
        r.value:: float as scale_score,
        submitted_at as measurement_date,
        false as is_baseline,
        true as is_progress
    from assessments join lateral flatten (input => assessments.responses) as r
    where assessments.type = 'Assessments::CareWellbeingCheckinAssessment'
        and assessments.submitted_at is not null
        and r.path in (
          'emotional_regulation_score',
          'optimism_score',
          'purpose_score',
          'resilience_score',
          'self_awareness_score',
          'score'
        )

),


distinct_measurement_dates as (

    select distinct user_id, to_date(measurement_date) as measurement_day from scores

),


next_measurement_dates as (

    select *, lead(measurement_day) ignore nulls over (partition by user_id order by measurement_day) as next_measurement_date
    from distinct_measurement_dates

)


, final as (

  select
      {{ dbt_utils.surrogate_key(['scores.user_id', 'scores.assessment_id', 'scores.construct']) }} as member_assessment_construct_key,
      scores.user_id,
      scores.assessment_id,
      scores.construct,
      scores.scale_score,
      scores.measurement_date,
      scores.is_baseline,
      scores.is_progress,
      next_measurement_dates.next_measurement_date,
      has_baseline.member_id is not null as has_baseline,
      most_recent_progress.last_progress_at is not null as has_progress,
      most_recent_progress.last_progress_at is null
          or scores.measurement_date = most_recent_progress.last_progress_at as is_most_recent,
      case
        when scores.scale_score < 38 then '1 - Stuck'
        when scores.scale_score < 52 then '2 - Strained'
        when scores.scale_score < 67 then '3 - Steady'
        when scores.scale_score <= 100 then '4 - Strong'
        else null
      end as well_being_cluster
  from scores
  left outer join next_measurement_dates on scores.user_id = next_measurement_dates.user_id
      and to_date(scores.measurement_date) = next_measurement_dates.measurement_day
  inner join has_baseline on scores.user_id = has_baseline.member_id
  left outer join most_recent_progress on scores.user_id = most_recent_progress.user_id
  --Band-aid solution: fanout should be handled in CTEs or upstream
  group by 1,2,3,4,5,6,7,8,9,10,11,12

)

select * from final
