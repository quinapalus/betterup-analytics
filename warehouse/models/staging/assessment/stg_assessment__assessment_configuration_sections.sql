WITH assessment_configuration_sections AS (

  SELECT * FROM {{ source('assessment', 'assessment_configuration_sections') }}

)

SELECT
  ID AS assessment_configuration_section_id,
  assessment_configuration_id,
  assessment_section_id,
  position,
  {{ load_timestamp('created_at') }},
  {{ load_timestamp('updated_at') }}

FROM assessment_configuration_sections

