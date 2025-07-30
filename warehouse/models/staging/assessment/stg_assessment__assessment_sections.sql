WITH assessment_sections AS (

  SELECT * FROM {{ source('assessment', 'assessment_sections') }}

)

SELECT
  id as assessment_section_id,
  key,
  {{ load_timestamp('created_at') }},
  {{ load_timestamp('updated_at') }}
FROM assessment_sections

