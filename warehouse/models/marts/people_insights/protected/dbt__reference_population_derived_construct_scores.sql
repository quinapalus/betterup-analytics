WITH construct_items AS (

  SELECT * FROM {{ ref('stg_assessment__construct_items') }}

),

construct_assessments AS (

  SELECT * FROM {{ ref('stg_assessment__construct_assessments') }}

),

construct_distributions AS (

  SELECT * FROM {{ ref('dbt__construct_distributions') }}

),

track_assignments AS (

  SELECT * FROM {{ ref('stg_app__track_assignments') }}

),

tracks AS (

  SELECT * FROM {{ ref('dim_tracks') }}

),

users AS (

  SELECT * FROM {{ ref('stg_app__users') }}

),

member_profiles AS (

  SELECT * FROM {{ ref('stg_app__member_profiles') }}

),

assessments AS (

  SELECT * FROM {{ ref('stg_app__assessments') }} WHERE submitted_at IS NOT NULL

),

reference_population_subgroups AS (

  SELECT * FROM {{ ref('stg_assessment__reference_population_subgroups') }}

),

whole_person_subdimensions AS (

  SELECT * FROM {{ ref('dbt__whole_person_subdimensions') }}

),

whole_person_dimension_scores AS (

  SELECT * FROM {{ ref('dbt__whole_person_dimension_scores') }}

),

assessment_constructs AS (

  -- Find all potential constructs based on assessment type, defined in the assessment_scoring gSheet.
  -- In a later query we'll filter for assessments that actually have all the required items for a given construct
  SELECT
    a.assessment_id,
    ca.construct_id
  FROM assessments AS a
  INNER JOIN construct_assessments AS ca
  ON a.type = ca.assessment_type

),

assessment_non_dimension_responses AS (

  -- unnest each item_key/response pair into separate rows
  SELECT
    a.assessment_id,
    r.path AS item_key,
    r.value::STRING AS item_response
  FROM assessments AS a
  JOIN LATERAL FLATTEN (input => a.responses) AS r

),

-- Pull in dimension scores as faux item responses for well_being calculation
assessment_dimension_scores AS (

  SELECT
    assessment_id,
    'dimension_score_' || dimension_key AS item_key,
    scale_score::STRING AS item_response
  FROM whole_person_dimension_scores
  WHERE whole_person_model_version = 'WPM 2.0'

),

assessment_item_responses AS (

  SELECT * FROM assessment_non_dimension_responses
  UNION ALL
  SELECT * FROM assessment_dimension_scores

),

assessment_construct_item_responses AS (

  -- Create a table with one row per item associated with a construct, for each assessment from above
  SELECT
    ac.assessment_id,
    ac.construct_id,
    ci.key,
    TRY_CAST(air.item_response AS NUMBER) AS item_response,
    CASE
      WHEN ci.is_scale_inverted THEN
        -- Inverted Score = Scale Score Length + 1 - Item Response
        ci.scale_length + 1 - TRY_CAST(air.item_response AS NUMBER)
      ELSE
        -- assume 'direct' item score scale type
        TRY_CAST(air.item_response AS NUMBER)
      END AS item_score
  FROM assessment_constructs AS ac
  INNER JOIN assessments AS a
    ON ac.assessment_id = a.assessment_id
  INNER JOIN users AS u
    ON a.user_id = u.user_id
  LEFT OUTER JOIN member_profiles AS mp
    ON u.member_profile_id = mp.member_profile_id
  INNER JOIN construct_items AS ci
    ON ac.construct_id = ci.construct_id
  -- left join so we get NULL values if an item is part of a construct but not included
  -- in the item responses for a given assessment
  LEFT OUTER JOIN assessment_item_responses AS air
    ON ac.assessment_id = air.assessment_id AND ci.key = air.item_key
  LEFT OUTER JOIN reference_population_subgroups rps
    ON ci.reference_population_subgroup_id = rps.reference_population_subgroup_id
  WHERE ci.reference_population_subgroup_id IS NULL
    OR (mp.people_manager AND rps.key = 'manager')
    OR (NOT COALESCE(mp.people_manager, FALSE) AND rps.key = 'individual_contributor' )
),

assessment_construct_raw_scores AS (

  -- aggregate to construct level and calculate raw scores
  SELECT
    assessment_id,
    construct_id,
    COUNT(construct_id) AS construct_item_count,
    COUNT(item_response) AS item_response_count,
    AVG(item_score) AS raw_score,
    SUM(item_score) AS raw_item_score_sum
  FROM assessment_construct_item_responses
  GROUP BY assessment_id, construct_id
  -- Filter out all assessment construct scores that don't have all required items
  HAVING construct_item_count = item_response_count

)

-- calculate scale scores
SELECT
  {{ dbt_utils.surrogate_key(['rs.assessment_id', 'cd.construct_id', 'cd.construct_reference_population_id', 'cd.reference_population_id']) }} as primary_key,
  rs.assessment_id,
  cd.construct_key,
  cd.construct_name as name,
  cd.label_i18n,
  rs.construct_id,
  cd.construct_reference_population_id,
  cd.construct_reference_population_uuid,
  cd.reference_population_id,
  cd.reference_population_subgroup_id, -- should always be null I think
  -- can I delete these?
  cd.reference_population_key,
  cd.reference_population_subgroup_key,
  rs.raw_score,
  rs.raw_item_score_sum,
  cd.score_mean,
  cd.score_standard_deviation,
  -- Scale z_score to desired mean and standard deviation, bounded by min and max scores defined in the constructs table
  GREATEST(cd.scale_min_score,
      LEAST(cd.scale_max_score,
        ((rs.raw_score - cd.score_mean) / cd.score_standard_deviation) * cd.scale_standard_deviation + cd.scale_mean
      )
  ) AS scale_score
FROM assessment_construct_raw_scores AS rs
INNER JOIN assessments AS a ON rs.assessment_id = a.assessment_id
INNER JOIN track_assignments AS ta ON a.track_assignment_id = ta.track_assignment_id
INNER JOIN tracks AS t ON ta.track_id = t.track_id
FULL OUTER JOIN construct_distributions AS cd
  ON cd.construct_id = rs.construct_id
  --derived constructs aren't suffixed with subgroups by assessments
  AND cd.reference_population_subgroup_id IS NULL
LEFT OUTER JOIN whole_person_subdimensions AS wps ON wps.construct_id = rs.construct_id
-- Exclude WPM subdimension constructs
WHERE wps.construct_id IS NULL
