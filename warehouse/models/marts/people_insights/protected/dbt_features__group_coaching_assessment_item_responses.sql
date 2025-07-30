WITH group_coaching_assessments AS (

  SELECT * FROM {{ ref('stg_app__assessments') }}
  WHERE type = 'Assessments::PostGroupCoachingSessionAssessment'

),

assessment_items AS (

  SELECT * FROM {{ ref('stg_assessment__assessment_items') }}

),

assessment_response_sets AS (

  SELECT * FROM {{ ref('stg_assessment__assessment_response_sets') }}

),

assessment_response_options AS (

  SELECT * FROM {{ ref('stg_assessment__assessment_response_options') }}

),

assessment_item_responses AS (
  SELECT
    a.assessment_id,
    a.user_id,
    a.creator_id,
    a.type,
    a.questions_version,
    a.created_at,
    a.submitted_at,
    r.path AS item_key,
    r.value::STRING AS item_response
  FROM group_coaching_assessments AS a
  INNER JOIN LATERAL FLATTEN (input => a.responses) AS r
  WHERE submitted_at IS NOT NULL
    -- discard empty or uninformative responses
    AND (r.value IS NOT NULL AND r.value != '')
)


SELECT DISTINCT
  -- Surrogate Key of Member ID, Associated Record ID, and Item Key
  {{ dbt_utils.surrogate_key(['ir.user_id', 'ir.assessment_id', 'ir.item_key']) }} AS member_record_item_id,
  ir.user_id AS member_id,
  ir.assessment_id AS associated_record_id,
  'Assessment' AS associated_record_type,
  ir.submitted_at AS feature_collected_at,
  ir.item_key AS feature_key,
  OBJECT_CONSTRUCT('translation_key', ro.translation_key, 'label', COALESCE(ro.label,ro.value)) AS classification,
  OBJECT_CONSTRUCT('intervention_type', 'coaching_circle') AS feature_attributes,
  'satisfaction' AS feature_type
FROM assessment_item_responses AS ir
INNER JOIN assessment_items AS i
  ON ir.item_key = i.key AND
     i.key IN ('coaching_circle_session_overall','coaching_circle_session_effective','coaching_circle_session_insight')
INNER JOIN assessment_response_options AS ro
  ON i.assessment_response_set_id = ro.assessment_response_set_id AND
     ir.item_response = ro.value