{{
  config(
    tags=['classification.c3_confidential']
  )
}}

WITH transitions AS (

  SELECT * FROM {{ source('jira', 'issue_transitions') }}

)


SELECT
  id AS jira_issue_transition_id,
  issueId AS jira_issue_id,
  name AS from_status,
  "TO":name AS to_status,
  "TO":statusCategory:name AS to_status_category
FROM transitions
