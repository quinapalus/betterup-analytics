{{
  config(
    tags=['classification.c3_confidential']
  )
}}

WITH completed_parent_assessment AS (

  SELECT * FROM {{ref('fact_whole_person_assessment')}}
  WHERE assessment_name IN ('WPM 1.0 180', 'WPM 1.0 360', 'WPM 2.0 180', 'WPM 2.0 360')

),

assessment_contributors AS (
  -- filter to contributor responses that were included in member-facing report:
  SELECT * FROM {{ref('dei_assessment_contributors')}}
  WHERE report_generated_at IS NOT NULL
    AND response_submitted_at IS NOT NULL
    AND response_submitted_at < report_generated_at

),

whole_person_subdimension_scores AS (
  -- filter scores to only include subdimensions that should be included in partner 360 reporting:
  SELECT * FROM {{ref('dei_whole_person_subdimension_scores')}}
  WHERE whole_person_subdimension_key IN (
    SELECT whole_person_subdimension_key
    FROM {{ref('dim_whole_person_subdimension')}}
    WHERE subdimension_is_included_in_partner_360_reporting
  )

),

contributor_group_subdimension_scores AS (

  SELECT
    ac.assessment_id AS parent_assessment_id,
    ss.whole_person_subdimension_key,
    ac.role AS contributor_role,
    AVG(ss.scale_score) AS scale_score_mean,
    COUNT(ac.contributor_id) AS contributor_count
  FROM assessment_contributors AS ac
  INNER JOIN whole_person_subdimension_scores AS ss
    ON ac.response_assessment_id = ss.assessment_id
  GROUP BY ac.assessment_id, ss.whole_person_subdimension_key, ac.role

),

renamed as (
SELECT
  cpa.member_key,
  cpa.date_key,
  cpa.account_key,
  cpa.deployment_key,
  cpa.member_deployment_key,
  self_report.whole_person_subdimension_key,
  cpa.assessment_name,
  self_report.scale_score AS self_report_scale_score,
  manager.contributor_count AS manager_count,
  manager.scale_score_mean AS manager_scale_score_mean,
  (self_report.scale_score - manager.scale_score_mean)
    AS self_report_estimation_relative_to_manager,
  direct_report.contributor_count AS direct_report_count,
  direct_report.scale_score_mean AS direct_report_scale_score_mean,
  (self_report.scale_score - direct_report.scale_score_mean)
    AS self_report_estimation_relative_to_direct_reports,
  other_coworker.contributor_count AS other_coworker_count,
  other_coworker.scale_score_mean AS other_coworker_scale_score_mean,
  (self_report.scale_score - other_coworker.scale_score_mean)
    AS self_report_estimation_relative_to_other_coworkers,
  cpa.account_assessment_sequence,
  cpa.deployment_assessment_sequence,
  cpa.account_wpm_sequence,
  cpa.deployment_wpm_sequence,
  cpa.deployment_assessment_reverse_sequence,
  cpa.app_assessment_id
FROM completed_parent_assessment AS cpa
INNER JOIN whole_person_subdimension_scores AS self_report
  ON cpa.app_assessment_id = self_report.assessment_id
-- Use INNER JOIN as all 180/360's require contribution from manager:
INNER JOIN contributor_group_subdimension_scores AS manager
  ON self_report.assessment_id = manager.parent_assessment_id AND
     manager.contributor_role = 'manager' AND
     self_report.whole_person_subdimension_key = manager.whole_person_subdimension_key
-- Use LEFT JOIN as not all 180/360's require contribution from direct_reports/other_coworkers:
LEFT OUTER JOIN contributor_group_subdimension_scores AS direct_report
  ON self_report.assessment_id = direct_report.parent_assessment_id AND
     direct_report.contributor_role = 'direct_report' AND
     self_report.whole_person_subdimension_key = direct_report.whole_person_subdimension_key
LEFT OUTER JOIN contributor_group_subdimension_scores AS other_coworker
  ON self_report.assessment_id = other_coworker.parent_assessment_id AND
     other_coworker.contributor_role = 'other_coworker' AND
     self_report.whole_person_subdimension_key = other_coworker.whole_person_subdimension_key
),

final as (
  select
  {{dbt_utils.surrogate_key(['member_key', 'date_key', 'account_key', 
                            'deployment_key', 'member_deployment_key', 
                            'whole_person_subdimension_key', 'app_assessment_id'])}} as _unique,
    *
  from renamed
)

select * from final