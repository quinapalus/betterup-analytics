WITH reference_population_subdimension_scores AS (

  SELECT * FROM {{ ref('dbt__reference_population_subdimension_scores') }}

),

reference_population_dimension_scores AS (

  SELECT * FROM {{ ref('dbt__reference_population_dimension_scores') }}

),

reference_population_derived_construct_scores AS (

  SELECT * FROM {{ ref('dbt__reference_population_derived_construct_scores') }}

),

reference_population_domain_scores AS (

  SELECT * FROM {{ ref('dbt__reference_population_domain_scores') }}

),

reference_population_persisted_construct_scores_non_subdimension AS (

  SELECT * FROM {{ ref('dbt__reference_population_persisted_construct_scores_non_subdimension') }}

),

assessments AS (

  SELECT * FROM {{ ref('fact_assessments') }}

),

track_assignments AS (

  SELECT * FROM {{ ref('stg_app__track_assignments') }}

),

tracks AS (

  SELECT * FROM {{ ref('dim_tracks') }}

),

members AS (

    SELECT * FROM {{ ref('dim_members') }}

),

organizations AS (

    SELECT * FROM {{ ref('stg_app__organizations') }}

),

reference_population_construct_scores AS (

SELECT
    construct_reference_population_id,
    construct_reference_population_uuid,
    reference_population_id,
    reference_population_subgroup_id,
    assessment_id,
    subdimension_key AS construct_key,
    name,
    label_i18n,
    PARSE_JSON('{
      "model": "' || whole_person_model_version || '",
      "construct_type": "subdimension",
      "subdimension": "' || subdimension_key || '",
      "subdimension_name": "' || name || '",
      "dimension": "' || dimension_key || '",
      "dimension_name": "' || dimension || '",
      "domain": "' || domain_key || '",
      "domain_name": "' || domain || '",
      "category": "' || category_key || '",
      "category_name": "' || category || '"
      }') AS construct_attributes,
    raw_score,
    score_mean,
    score_standard_deviation,
    original_scale_score,
    scale_score,
    'subdimension' AS construct_type
  FROM reference_population_subdimension_scores

  UNION ALL

  SELECT
    NULL as construct_reference_population_id,
    NULL as construct_reference_population_uuid,
    reference_population_id,
    reference_population_subgroup_id,
    assessment_id,
    dimension_key AS construct_key,
    dimension as name,
    NULL as label_i18n,
    PARSE_JSON('{
      "model": "' || whole_person_model_version || '",
      "construct_type": "dimension",
      "dimension": "' || dimension_key || '",
      "dimension_name": "' || dimension || '",
      "domain": "' || domain_key || '",
      "domain_name": "' || domain || '",
      "category": "' || category_key || '",
      "category_name": "' || category || '"
      }') AS construct_attributes,
    raw_score,
    -- Since dimensions are aggregates of subdimensions, these metrics don't provide any value
    NULL AS score_mean,
    NULL AS score_standard_deviation,
    original_scale_score,
    scale_score,
    'dimension' AS construct_type
  FROM reference_population_dimension_scores

  UNION ALL

  SELECT
    construct_reference_population_id,
    construct_reference_population_uuid,
    reference_population_id,
    reference_population_subgroup_id,
    assessment_id,
    construct_key,
    name,
    label_i18n,
    PARSE_JSON('{
      "model": "WPM 2.0",
      "construct_type": "construct",
      "reference_population": "' || IFNULL(reference_population_key, '') || '",
      "reference_population_subgroup": "' || IFNULL(reference_population_subgroup_key, '') || '"
      }') AS construct_attributes,
    raw_score,
    score_mean,
    score_standard_deviation,
    -- Same as it's always been
    scale_score AS original_scale_score,
    scale_score,
   'construct' AS construct_type
  FROM reference_population_derived_construct_scores

  UNION ALL

    SELECT
    NULL as construct_reference_population_id,
    NULL as construct_reference_population_uuid,
    reference_population_id,
    reference_population_subgroup_id,
    assessment_id,
    domain_key AS construct_key,
    domain as name,
    NULL AS label_i18n,
    PARSE_JSON('{
      "domain": "' || domain_key || '",
      "domain_name": "' || domain || '",
      "category": "' || category_key || '",
      "category_name": "' || category || '"
      }') AS construct_attributes,
    raw_score,
    -- Since dimensions are aggregates of subdimensions, these metrics don't provide any value
    NULL AS score_mean,
    NULL AS score_standard_deviation,
    original_scale_score,
    scale_score,
    'domain' AS construct_type
    FROM reference_population_domain_scores

   UNION ALL

    SELECT
    construct_reference_population_id,
    construct_reference_population_uuid,
    reference_population_id,
    reference_population_subgroup_id,
    assessment_id,
    key AS construct_key,
    name,
    label_i18n,
    NULL AS construct_attributes,
    raw_score,
    score_mean,
    score_standard_deviation,
    original_scale_score,
    scale_score,
    'persisted_construct' AS construct_type
    FROM reference_population_persisted_construct_scores_non_subdimension
)

SELECT
  {{ dbt_utils.surrogate_key(['s.assessment_id', 's.construct_key', 's.construct_type', 's.reference_population_id', 's.reference_population_subgroup_id']) }} AS primary_key,
  s.construct_reference_population_id,
  s.construct_reference_population_uuid,
  s.reference_population_id,
  s.reference_population_subgroup_id,
  a.user_id AS member_id,
  a.track_assignment_id,
  ta.track_id,
  s.construct_key,
  s.name,
  s.label_i18n,
  s.construct_type,
  s.raw_score,
  s.score_mean AS scale_score_mean,
  s.score_standard_deviation AS scale_score_standard_deviation,
  s.original_scale_score,
  -- Fall back to original score - This should only happen for 'turnover_intentions'
  IFF(s.construct_key = 'turnover_intentions', COALESCE(s.scale_score, s.original_scale_score), s.scale_score) AS scale_score,
  s.construct_attributes,
  s.assessment_id,
  a.submitted_at,
  a.associated_record_id,
  a.associated_record_type,
  o.organization_id,
  o.reference_population_id AS organization_reference_population_id,
  -- the following denormalized fields included to make filtering on Looker derived tables more performant
  a.assessment_type,
  a.assessment_name,
  a.assessment_configuration_id,
  a.title_en,
  a.is_primary_reflection_point,
  a.is_onboarding,
  a.is_duplicate_assessment,
  t.name as track_name,
  t.program_name,
  t.deployment_group,
  o.name as organization_name,

  -- lifetime program access fields for the member, used in track/RG filtering
  m.lifetime_track_ids,
  m.lifetime_track_names,
  m.lifetime_reporting_group_ids,

  --sequence
  assessment_sequence_by_onboarding_or_rp,
  assessment_reverse_sequence_by_onboarding_or_rp,
  dense_rank() over (partition by a.user_id, s.construct_type, s.construct_key, s.reference_population_id, a.is_onboarding, a.is_primary_reflection_point order by a.submitted_at) as construct_sequence_by_onboarding_or_rp,
  dense_rank() over (partition by a.user_id, s.construct_type, s.construct_key, s.reference_population_id, a.is_onboarding, a.is_primary_reflection_point order by a.submitted_at desc) as construct_reverse_sequence_by_onboarding_or_rp,

  -- sequence fields for data scientists, "rigid" because not dynamic to track/reporting group filtering like ones in Looker
  ROW_NUMBER() OVER (PARTITION BY a.user_id, s.construct_type, s.construct_key, s.reference_population_id ORDER BY a.submitted_at) AS score_sequence_rigid,
  ROW_NUMBER() OVER (PARTITION BY a.user_id, s.construct_type, s.construct_key, s.reference_population_id ORDER BY a.submitted_at DESC) AS score_reverse_sequence_rigid
FROM reference_population_construct_scores AS s
INNER JOIN assessments AS a ON s.assessment_id = a.assessment_id
INNER JOIN track_assignments AS ta ON a.track_assignment_id = ta.track_assignment_id
INNER JOIN tracks AS t ON ta.track_id = t.track_id
LEFT JOIN members m on a.user_id = m.member_id
LEFT JOIN organizations o on t.organization_id = o.organization_id
