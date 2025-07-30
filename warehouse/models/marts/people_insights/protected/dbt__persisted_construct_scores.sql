WITH assessments AS (

  SELECT * FROM {{ ref('stg_app__assessments') }}

),

scores AS (

  SELECT * FROM {{ ref('stg_app__scores') }}

),

track_assignments AS (

  SELECT * FROM {{ ref('stg_app__track_assignments') }}

),

tracks AS (

  SELECT * FROM {{ ref('dim_tracks') }}

),

organization_constructs AS (

  SELECT * FROM {{ ref('dbt__organization_constructs') }}

),

construct_distributions AS (

  SELECT * FROM {{ ref('dbt__construct_distributions') }}

),

persisted_scores AS (
  SELECT
    {{ dbt_utils.surrogate_key(['a.assessment_id', 's.key', 'cd.construct_id']) }} as primary_key,
    a.assessment_id,
    cd.construct_id,
    s.key,
    --adding these fields for filters in later models
    a.type as assessment_type,
    a.questions_version,
    s.type,
    -- score fields
    s.raw_score,
    -- For partner reporting, we want to override the score's original reference population and re-calculate the scale scores
    -- using the reference population that is configured on the organization level, if available.
    -- If for some reason there is no construct_reference_population populated yet, fall back on what was used at submission time.
    -- This protects us from the case where we add a new reference population for an org but don't have a matching distribution.
    COALESCE(oc.construct_reference_population_id, cd.construct_reference_population_id) AS construct_reference_population_id,
    COALESCE(oc.construct_reference_population_uuid, cd.construct_reference_population_uuid) AS construct_reference_population_uuid,
    COALESCE(oc.score_mean, cd.score_mean) AS score_mean,
    COALESCE(oc.score_standard_deviation, cd.score_standard_deviation) AS score_standard_deviation,
    COALESCE(oc.scale_min_score, cd.scale_min_score) AS scale_min_score,
    COALESCE(oc.scale_max_score, cd.scale_max_score) AS scale_max_score,
    COALESCE(oc.scale_mean, cd.scale_mean) AS scale_mean,
    COALESCE(oc.scale_standard_deviation, cd.scale_standard_deviation) AS scale_standard_deviation,
    s.scale_score AS original_scale_score,
    -- Fall back to old scale score if we're missing a construct reference population
    GREATEST(COALESCE(oc.scale_min_score, cd.scale_min_score),
      LEAST(COALESCE(oc.scale_max_score, cd.scale_max_score),
        ((s.raw_score - COALESCE(oc.score_mean, cd.score_mean)) / COALESCE(oc.score_standard_deviation, cd.score_standard_deviation)) * COALESCE(oc.scale_standard_deviation, cd.scale_standard_deviation) + COALESCE(oc.scale_mean, cd.scale_mean)
      )
    ) AS scale_score
  FROM scores AS s
  INNER JOIN assessments AS a ON s.assessment_id = a.assessment_id
  INNER JOIN construct_distributions AS cd
    ON s.construct_reference_population_uuid = cd.construct_reference_population_uuid
  INNER JOIN track_assignments AS ta ON a.track_assignment_id = ta.track_assignment_id
  INNER JOIN tracks AS t ON ta.track_id = t.track_id
  LEFT OUTER JOIN organization_constructs AS oc
  ON oc.construct_id = cd.construct_id
    AND (
      oc.reference_population_subgroup_id = cd.reference_population_subgroup_id
      OR (oc.reference_population_subgroup_id IS NULL AND cd.reference_population_subgroup_id IS NULL)
    )
    AND oc.organization_id = t.organization_id
  WHERE
    -- filter for submitted Whole Person Model 2.0 assessments
    a.submitted_at IS NOT NULL

)


SELECT
  s.*
FROM persisted_scores AS s

