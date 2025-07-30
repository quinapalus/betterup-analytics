WITH assessment_items AS (

  SELECT * FROM {{ref('dei_assessment_items')}}

),

member_assessments AS (

  SELECT * FROM {{ref('dei_member_assessments')}}

),

model_definition_whole_person AS (

  SELECT * FROM {{ref('model_definition_whole_person')}}

),

wpm_assessment_items AS (

  SELECT
    *
  FROM assessment_items
  WHERE
    type IN ('Assessments::WholePersonAssessment',
             'Assessments::WholePersonProgramCheckinAssessment',
             'Assessments::WholePerson360Assessment',
             'Assessments::WholePerson360ContributorAssessment',
             'Assessments::WholePerson180Assessment',
             'Assessments::WholePerson180ContributorAssessment')
    AND (item_key LIKE 'subdimension_z_score_%' OR item_key LIKE 'category_z_score_%')
    AND created_at > '2017-02-04' -- Prior to this date WPM was on a 7 point scale

)


SELECT
  {{ dbt_utils.surrogate_key(['wa.assessment_id', 'md.score_key']) }} AS primary_key,
  wa.type AS source,
  wa.assessment_id,
  wa.created_at,
  ma.questions_version,
  md.score_key,
  md.score_type,
  md.score_title,
  wa.item_response::float AS z_score,
  wa.sequence,  -- Sequence iterates within each source
  wa.submitted_at,
  wa.user_id AS member_id,
  ma.track_id,
  md.subdimension,
  md.dimension,
  md.factor
FROM wpm_assessment_items AS wa
INNER JOIN member_assessments AS ma
  ON wa.assessment_id = ma.assessment_id
INNER JOIN model_definition_whole_person AS md
  ON SPLIT_PART(wa.item_key, '_z_score_', 2) = md.score_key
