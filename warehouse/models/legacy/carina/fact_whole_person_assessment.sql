{{
  config(
    tags=['classification.c3_confidential','eu']
  )
}}

WITH member_assessments AS (

  SELECT * FROM {{ref('dei_member_assessments')}}

),

tracks AS (

  SELECT * FROM {{ref('dim_tracks')}} 
  where is_external and engaged_member_count is not null --this logic was in dei_tracks which this model used to reference

),

dim_account as (

  select * from {{ ref('dim_account') }}

),

dim_member as (

  select * from {{ ref('dim_member') }}

),

dim_deployment as (

  select * from {{ ref('dim_deployment') }}

),

final as (

  SELECT
    {{ member_key('ma.member_id') }} AS member_key,
    {{ date_key('ma.report_generated_at') }} AS date_key,
    {{ account_key('t.organization_id', 't.sfdc_account_id') }} AS account_key,
    {{ deployment_key('ma.track_id') }} AS deployment_key,
    {{ member_deployment_key('ma.member_id', 'ma.track_id') }} AS member_deployment_key,
    ma.type AS assessment_type,
    {{ sanitize_wpm_assessment_type ('ma.type', 'ma.questions_version') }} AS assessment_name,
    {{ sanitize_wpm_model_version ('ma.questions_version') }} AS whole_person_model_version,
    ma.assessment_id AS app_assessment_id,
    ma.report_generated_at,

    ROW_NUMBER() OVER (PARTITION BY member_key, account_key, assessment_name ORDER BY ma.report_generated_at)
      AS account_assessment_sequence,
    ROW_NUMBER() OVER (PARTITION BY member_key, deployment_key, assessment_name ORDER BY ma.report_generated_at)
      AS deployment_assessment_sequence,
    ROW_NUMBER() OVER (PARTITION BY member_key, account_key, whole_person_model_version ORDER BY ma.report_generated_at)
      AS account_wpm_sequence,
    ROW_NUMBER() OVER (PARTITION BY member_key, whole_person_model_version ORDER BY ma.report_generated_at)
      AS member_wpm_sequence,
    ROW_NUMBER() OVER (PARTITION BY member_key, deployment_key, whole_person_model_version ORDER BY ma.report_generated_at)
      AS deployment_wpm_sequence,
    ROW_NUMBER() OVER (PARTITION BY member_key, deployment_key, assessment_name ORDER BY ma.report_generated_at DESC)
      AS deployment_assessment_reverse_sequence

  FROM member_assessments AS ma
  INNER JOIN tracks AS t
    ON ma.track_id = t.track_id
  WHERE
    ma.type IN ('Assessments::WholePersonAssessment',
                'Assessments::WholePersonProgramCheckinAssessment',
                'Assessments::WholePerson180Assessment',
                'Assessments::WholePerson360Assessment',
                'Assessments::WholePersonGroupCoachingCheckinAssessment')
    AND ma.report_generated_at IS NOT NULL
    AND ma.created_at > '2017-02-04') -- prior to this date WPM was on a 7 point scale

select
  final.*,
  dm.app_member_id as member_id,
  dd.app_track_id as track_id,  
  --wpm assessment flags
  iff(assessment_name like '% Baseline', true, false) as is_baseline_assessment,
  iff(assessment_name like '%Reflection Point', true, false) as is_any_reflection_point,
  iff(assessment_name in ('WPM 1.0 Reflection Point','WPM 2.0 Reflection Point'), true, false) as is_primary_coaching_reflection_point,
  iff(assessment_name = 'WPM 2.0 Group Coaching Reflection Point', true, false) as is_group_coaching_reflection_point,
  iff(whole_person_model_version = 'WPM 1.0', true, false) as is_wpm_1,
  iff(whole_person_model_version = 'WPM 2.0', true, false) as is_wpm_2,
  iff(is_baseline_assessment and deployment_assessment_sequence = 1, true, false) as is_first_baseline_assessment,
  iff(is_primary_coaching_reflection_point and deployment_assessment_sequence = 1, true, false) as is_first_primary_coaching_reflection_point,
  iff(is_group_coaching_reflection_point and deployment_assessment_sequence = 1, true, false) as is_first_group_coaching_reflection_point,
  iff(is_primary_coaching_reflection_point and deployment_assessment_reverse_sequence = 1, true, false) as is_most_recent_primary_coaching_reflection_point,
  iff(is_group_coaching_reflection_point and deployment_assessment_reverse_sequence = 1, true, false) as is_most_recent_group_coaching_reflection_point,
  iff((is_primary_coaching_reflection_point or is_group_coaching_reflection_point) and deployment_assessment_reverse_sequence = 1, true, false) as is_most_recent_reflection_point
from final
  INNER JOIN dim_account AS da ON final.account_key = da.account_key
  INNER JOIN dim_member AS dm ON final.member_key = dm.member_key
  INNER JOIN dim_deployment AS dd ON final.deployment_key = dd.deployment_key
