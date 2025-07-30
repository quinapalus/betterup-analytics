{{
  config(
    tags=['classification.c3_confidential'],
    materialized='view'
  )
}}

WITH assessment_items AS (

  SELECT * FROM {{ref('dei_assessment_items')}}

),

member_assessments AS (

  SELECT * FROM {{ref('dei_member_assessments')}}

),

assessment_contributors AS (

  SELECT * FROM {{ref('dei_assessment_contributors')}}

),

tracks AS (

  SELECT * FROM {{ref('dim_tracks')}} 
  WHERE is_external and engaged_member_count is not null --this logic was in dei_tracks which this model used to reference

),

item_definition_whole_person AS (

  SELECT * FROM {{ref('item_definition_whole_person')}}

),

dim_member AS (

  SELECT * FROM {{ref('dim_members')}}

),

dim_account AS (

  SELECT * FROM {{ref('dim_account')}}

),

wpm_assessments_partitioned_by_creator AS (

  SELECT
    -- extract only self-reported submitted member WPM assessments
    assessment_id AS app_member_assessment_id,
    assessment_id AS app_creator_assessment_id, -- member is the creator as well in this case
    member_id,
    track_id,
    type,
    questions_version,
    'self' AS creator_role,
    submitted_at AS app_creator_submitted_at
  FROM member_assessments
  WHERE
    type IN ('Assessments::WholePersonAssessment',
             'Assessments::WholePersonProgramCheckinAssessment',
             'Assessments::WholePerson360Assessment',
             'Assessments::WholePerson180Assessment')
    AND created_at > '2017-02-04' -- prior to this date WPM was on a 7 point scale

  UNION

  SELECT
    -- extract only contributor-reported member WPM assessments
    ca.assessment_id AS app_member_assessment_id,
    ca.response_assessment_id AS app_creator_assessment_id,
    ma.member_id,
    ma.track_id,
    ma.type,
    ma.questions_version,
    ca.role AS creator_role,
    ca.response_submitted_at AS app_creator_submitted_at
  FROM member_assessments AS ma
  INNER JOIN assessment_contributors AS ca
    ON ma.assessment_id = ca.response_assessment_id
  WHERE
    -- source the type from member assessments since contributor
    -- table is not associated with an assessment type directly
    ma.type IN ('Assessments::WholePerson360ContributorAssessment',
                'Assessments::WholePerson180ContributorAssessment')
    AND ma.created_at > '2017-02-04'
    -- filter for submitted contributor assessments
    AND ca.response_submitted_at IS NOT NULL

)


SELECT
  -- Surrogate Primary Key composed of MEMBER_KEY, ACCOUNT_KEY, DEPLOYMENT_KEY, APP_MEMBER_ASSESSMENT_ID, APP_CREATOR_ASSESSMENT_ID, and ITEM_KEY
  {{ dbt_utils.surrogate_key(['wa.member_id', 't.organization_id', 't.sfdc_account_id', 'wa.track_id', 'wa.app_member_assessment_id','wa.app_creator_assessment_id','ai.item_key']) }} as id,
  {{ member_key ('wa.member_id') }} AS member_key,
  {{ date_key('wa.app_creator_submitted_at') }} AS date_key,
  {{ account_key('t.organization_id', 't.sfdc_account_id') }} AS account_key,
  {{ deployment_key('wa.track_id') }} AS deployment_key,
  wa.app_member_assessment_id,
  wa.app_creator_assessment_id,
  {{ sanitize_wpm_assessment_type ('wa.type', 'wa.questions_version') }} AS assessment_name,
  wa.creator_role,
  wa.app_creator_submitted_at,
  ai.item_key,
  ai.item_response
FROM wpm_assessments_partitioned_by_creator AS wa
INNER JOIN assessment_items AS ai
  -- join through creator's assessment ID to identify contributors' item responses
  ON wa.app_creator_assessment_id = ai.assessment_id
  AND ai.item_key IN (SELECT item_key FROM item_definition_whole_person)
INNER JOIN tracks AS t
  ON wa.track_id = t.track_id
WHERE
  -- ensure foreign keys are present in dimension tables
  {{member_key ('wa.member_id')}} IN (SELECT member_key FROM dim_member) AND
  {{account_key('t.organization_id', 't.sfdc_account_id')}} IN (SELECT account_key FROM dim_account)
