WITH members AS (

  SELECT * FROM {{ref('dei_members')}}

),

activities AS (

  SELECT * FROM {{ref('dei_activities')}}

),

assessments AS (

  SELECT * FROM {{ref('int_app__assessments')}}

),

user_info AS (

  SELECT * FROM {{ref('stg_app__users')}}

),

managers AS (

  SELECT
    members.member_id,
    members.manager_id,
    managers.email AS manager_email
  FROM members
  INNER JOIN user_info AS managers ON members.manager_id = managers.user_id

),

activity_member_request AS (

  SELECT * FROM (

  SELECT
    member_id,
    created_at AS assigned_at,
    activity_id,
    -- return first activity assignment
    ROW_NUMBER() OVER (
        PARTITION BY member_id
        ORDER BY member_id, created_at
    ) AS index
  FROM activities
  WHERE resource_type = 'Resources::AssessmentResource'
  AND resource_content = 'Assessments::ManagerFeedbackRequestAssessment'

  ) a

  WHERE index = 1

),

member_request AS (

  SELECT * FROM (

  SELECT
    a.user_id AS member_id,
    managers.manager_id,
    a.assessment_id,
    a.submitted_at,
    -- select first member_request for each member-manager pair
    ROW_NUMBER() OVER (
        PARTITION BY a.user_id, managers.manager_id
        ORDER BY a.user_id, managers.manager_id, a.submitted_at
    ) AS index
  FROM assessments AS a
  INNER JOIN managers ON
    lower(a.responses:manager_email::VARCHAR) = managers.manager_email
    -- manager email input by member is sanitized by application before saving in users table
  WHERE a.type = 'Assessments::ManagerFeedbackRequestAssessment'

  ) a

  WHERE index = 1

),

manager_feedback AS (

  SELECT * FROM (

  SELECT
    a.user_id AS member_id,
    a.creator_id AS manager_id,
    a.assessment_id,
    a.submitted_at,
    -- select first manager_feedback for each member-manager pair
    ROW_NUMBER() OVER (
        PARTITION BY a.user_id, a.creator_id
        ORDER BY a.user_id, a.creator_id, a.submitted_at
    ) AS index
  FROM assessments AS a
  WHERE a.type = 'Assessments::ManagerFeedbackAssessment'

  ) a

  WHERE index = 1

)


SELECT
  m.member_id,
  m.manager_id,
  managers.manager_email,

  member_activity.assigned_at IS NOT NULL AS member_request_activity_assigned,
  member_activity.assigned_at AS member_request_activity_assigned_at,
  member_activity.activity_id AS member_request_activity_id,

  member_request.submitted_at IS NOT NULL AS member_request_submitted,
  member_request.submitted_at AS member_request_submitted_at,
  datediff('day', member_activity.assigned_at, member_request.submitted_at)
    AS member_request_submitted_days_after_activity,
  member_request.assessment_id AS member_request_assessment_id,

  manager_feedback.submitted_at IS NOT NULL AS manager_feedback_submitted,
  manager_feedback.submitted_at AS manager_feedback_submitted_at,
  datediff('day', member_request.submitted_at, manager_feedback.submitted_at)
    AS manager_feedback_submitted_days_after_request,
  manager_feedback.assessment_id AS manager_feedback_assessment_id
FROM members AS m
LEFT OUTER JOIN managers ON m.member_id = managers.member_id
LEFT OUTER JOIN activity_member_request AS member_activity
  ON m.member_id = member_activity.member_id
LEFT OUTER JOIN member_request ON m.member_id = member_request.member_id
  AND m.manager_id = member_request.manager_id
LEFT OUTER JOIN manager_feedback ON m.member_id = manager_feedback.member_id
  AND m.manager_id = manager_feedback.manager_id
