WITH manager_feedback_flows AS (

  SELECT * FROM {{ source('app', 'manager_feedback_flows') }}

)

SELECT
  id as manager_feedback_flow_id,
  manager_id,
  member_id, 
  assessment_type_chosen,
  feedback_request_assessment_id,
  manager_feedback_assessment_id,
  manager_growth_assessment_id,
  onboarding_assessment_id,
  track_assignment_id,
  {{ load_timestamp('growth_due_on') }},
  {{ load_timestamp('recommendation_due_on') }},
  {{ load_timestamp('opted_out_at') }},
  {{ load_timestamp('relationship_ended_at') }},
  {{ load_timestamp('created_at') }},
  {{ load_timestamp('updated_at') }}
FROM manager_feedback_flows
