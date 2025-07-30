WITH assessment_section_items AS (

  SELECT * FROM {{ source('assessment', 'assessment_section_items') }}

)

SELECT
  id as assessment_section_item_id,
  assessment_item_id,
  assessment_section_id,
  position,
  {{ load_timestamp('created_at') }},
  {{ load_timestamp('updated_at') }}
FROM assessment_section_items

