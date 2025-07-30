WITH assessment_contributors AS (

  SELECT * FROM {{ref('stg_app__assessment_contributors')}}

),

assessments AS (
  --using staging model vs int model to include records where submitted at IS NULL
  SELECT * FROM {{ref('stg_app__assessments')}}

)


SELECT
  ac.assessment_contributor_id,
  ac.assessment_id,
  ac.role,
  ac.contributor_id,
  COALESCE(ac.response_assessment_id, unsubmitted_response.assessment_id)
    AS response_assessment_id,
  ac.created_at,
  COALESCE(response.created_at, unsubmitted_response.created_at)
    AS response_created_at,
  response.submitted_at AS response_submitted_at,
  parent.report_generated_at
FROM assessment_contributors AS ac
INNER JOIN assessments AS parent
  ON ac.assessment_id = parent.assessment_id
LEFT OUTER JOIN assessments AS response
  ON ac.response_assessment_id = response.assessment_id
LEFT OUTER JOIN assessments AS unsubmitted_response
  ON ac.assessment_id = unsubmitted_response.parent_id AND
     ac.contributor_id = unsubmitted_response.creator_id AND
     unsubmitted_response.submitted_at IS NULL
-- Remove any duplicate rows introduced when joining to unsubmitted responses:
QUALIFY ROW_NUMBER() OVER
  (PARTITION BY ac.assessment_contributor_id ORDER BY unsubmitted_response.created_at DESC) = 1
