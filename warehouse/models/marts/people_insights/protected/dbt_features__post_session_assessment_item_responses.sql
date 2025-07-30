WITH appointments AS (

  SELECT * FROM {{ ref('stg_app__appointments') }}

),

coach_assignments AS (

  SELECT * FROM {{ ref('stg_app__coach_assignments') }}

),

specialist_verticals AS (

  SELECT * FROM {{ ref('stg_curriculum__specialist_verticals') }}

),

assessments AS (

  SELECT * FROM {{ ref('stg_app__assessments') }}

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
    {{ dbt_utils.surrogate_key(['assessment_id', 'user_id', 'r.path', 'r.value']) }} as assessment_item_response_key,
    a.assessment_id,
    a.user_id,
    a.creator_id,
    a.type,
    a.questions_version,
    a.created_at,
    a.submitted_at,
    r.path AS item_key,
    r.value::STRING AS item_response
  FROM assessments AS a
  INNER JOIN LATERAL FLATTEN (input => a.responses) AS r
  WHERE submitted_at IS NOT NULL
    -- discard empty or uninformative responses
    AND (r.value IS NOT NULL AND r.value != '')
    AND a.type = 'Assessments::PostSessionMemberAssessment'
)

SELECT DISTINCT
  ir.assessment_item_response_key,
  ir.user_id AS member_id,
  ir.assessment_id AS associated_record_id,
  'Assessment' AS associated_record_type,
  ir.submitted_at AS feature_collected_at,
  ir.item_key AS feature_key,
  OBJECT_CONSTRUCT('translation_key', ro.translation_key, 'label', COALESCE(ro.label,ro.value)) AS classification,
  OBJECT_CONSTRUCT('appointment_id', a.appointment_id, 'coach_type', ca.role, 'extended_network_topic_key', sv.key,'extended_network_topic', sv.name) AS feature_attributes,
  'satisfaction' AS feature_type
FROM appointments AS a
INNER JOIN coach_assignments AS ca
  ON a.coach_assignment_id = ca.coach_assignment_id
LEFT JOIN specialist_verticals AS sv
  ON ca.specialist_vertical_uuid = sv.specialist_vertical_uuid
INNER JOIN assessment_item_responses AS ir
  ON a.post_session_member_assessment_id = ir.assessment_id
INNER JOIN assessment_items AS i
  ON ir.item_key = i.key AND
     i.translation_key IN ('post_session_member_session_overall_emotional', 'post_session_member_session_was_valuable', 'post_session_member_insight_rating', 'post_session_member_layer2_member_rated_goal_progress',
     'post_session_member_layer2_utility_rating')
INNER JOIN assessment_response_options AS ro
  ON i.assessment_response_set_id = ro.assessment_response_set_id AND
     ir.item_response = ro.value
