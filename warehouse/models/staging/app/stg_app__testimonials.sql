WITH testimonials AS (

  SELECT * FROM {{ source('app', 'testimonials') }}

)

SELECT
  id AS testimonial_id,
  text,
  assessment_id,
  {{ load_timestamp('created_at') }},
  {{ load_timestamp('updated_at') }}
FROM testimonials
