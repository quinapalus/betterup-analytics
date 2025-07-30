WITH assessments AS (

  SELECT * FROM {{ ref('int_app__assessments') }}

),

scores AS (

  SELECT * FROM {{ ref('int_app__scores') }}

),

track_assignments AS (

  SELECT * FROM {{ ref('stg_app__track_assignments') }}

),

tracks AS (

  SELECT * FROM {{ ref('dim_tracks') }}

),

construct_distributions AS (

  SELECT * FROM {{ ref('dbt__construct_distributions') }}

),

persisted_scores AS (
  SELECT
    {{ dbt_utils.surrogate_key(['a.assessment_id', 'cd_mapping.construct_id', 'cd_mapping.reference_population_id', 'cd_mapping.reference_population_subgroup_id', 'cd.construct_reference_population_id']) }} as primary_key,
     a.assessment_id,
    cd_mapping.construct_id,
    cd_mapping.construct_name as name,
    cd_mapping.label_i18n,
    s.key,
    cd.construct_reference_population_id,
    cd.construct_reference_population_uuid,
    cd.reference_population_id,
    cd_mapping.reference_population_subgroup_id,
    --adding these fields for filters in later models
    a.type as assessment_type,
    a.questions_version,
    s.type,
    -- score fields
    s.raw_score,
    cd.score_mean,
    cd.score_standard_deviation,
    cd.scale_min_score,
    cd.scale_max_score,
    cd.scale_mean,
    cd.scale_standard_deviation,
    s.scale_score AS original_scale_score,
    -- Scale z_score to desired mean and standard deviation, bounded by min and max scores defined in the constructs table
    GREATEST(cd.scale_min_score,
      LEAST(cd.scale_max_score,
        ((s.raw_score - cd.score_mean) / cd.score_standard_deviation) * cd.scale_standard_deviation + cd.scale_mean
      )
    ) AS scale_score

  FROM scores AS s
  INNER JOIN assessments AS a ON s.assessment_id = a.assessment_id
  -- Join construct_distributions as mapping table to get reference_population_subgroup_id
  INNER JOIN construct_distributions AS cd_mapping
    ON s.construct_reference_population_uuid = cd_mapping.construct_reference_population_uuid
  FULL OUTER JOIN construct_distributions AS cd
    ON cd_mapping.construct_id = cd.construct_id
    AND (
        cd_mapping.reference_population_subgroup_id = cd.reference_population_subgroup_id
        OR (cd_mapping.reference_population_subgroup_id IS NULL AND cd.reference_population_subgroup_id IS NULL)
    )
  INNER JOIN track_assignments AS ta ON a.track_assignment_id = ta.track_assignment_id
  INNER JOIN tracks AS t ON ta.track_id = t.track_id

)

SELECT
  s.*
FROM persisted_scores AS s