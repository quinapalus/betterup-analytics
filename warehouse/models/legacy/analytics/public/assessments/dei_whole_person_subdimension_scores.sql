{{
  config(
    tags=['classification.c3_confidential']
  )
}}

WITH wpm_assessment_items AS (

  SELECT * FROM {{ref('dei_assessment_items')}}
  WHERE type IN ('Assessments::WholePersonAssessment',
                 'Assessments::WholePersonProgramCheckinAssessment',
                 'Assessments::WholePerson360Assessment',
                 'Assessments::WholePerson360ContributorAssessment',
                 'Assessments::WholePerson180Assessment',
                 'Assessments::WholePerson180ContributorAssessment',
                 'Assessments::WholePersonGroupCoachingCheckinAssessment')

),

member_assessments AS (

  SELECT * FROM {{ref('dei_member_assessments')}}

),

whole_person_subdimensions AS (

  SELECT * FROM {{ref('dbt_whole_person_subdimensions')}}

),

staged_assessment_items AS (

  SELECT
    i.type AS source,
    i.assessment_id,
    i.questions_version,
    i.submitted_at,
    i.user_id AS member_id,
    a.track_id,
    i.item_key,
    i.item_response::float AS item_response
  FROM wpm_assessment_items AS i
  INNER JOIN member_assessments AS a
    ON i.assessment_id = a.assessment_id

),

wpm2_subdimension_scores AS (

  SELECT
    i.source,
    i.assessment_id,
    i.submitted_at,
    i.member_id,
    i.track_id,
    i.item_response AS scale_score,
    wps.assessment_subdimension_key,
    wps.whole_person_model_version,
    wps.whole_person_subdimension_key
  FROM staged_assessment_items AS i
  INNER JOIN whole_person_subdimensions AS wps
    ON wps.whole_person_model_version = 'WPM 2.0' AND
       -- strip leading "subdimension_score_" from item_key to join subdimension metadata
       SUBSTRING(i.item_key, 20) = wps.assessment_subdimension_key
  WHERE
    i.questions_version = '2.0'
    AND (i.item_key LIKE 'subdimension_score_%')

),

wpm1_subdimension_scores AS (

  SELECT
    i.source,
    i.assessment_id,
    i.submitted_at,
    i.member_id,
    i.track_id,
    {{ scale_z_score('i.item_response') }} AS scale_score,
    wps.assessment_subdimension_key,
    wps.whole_person_model_version,
    wps.whole_person_subdimension_key
  FROM staged_assessment_items AS i
  INNER JOIN whole_person_subdimensions AS wps
    ON wps.whole_person_model_version = 'WPM 1.0' AND
       -- strip leading "subdimension_z_score_" from item_key to join subdimension metadata
       SUBSTRING(i.item_key, 22) = wps.assessment_subdimension_key
  WHERE
    i.questions_version = '1.0'
    AND (i.item_key LIKE 'subdimension_z_score_%')

)


SELECT
  *
FROM wpm2_subdimension_scores

UNION ALL

SELECT
  *
FROM wpm1_subdimension_scores
