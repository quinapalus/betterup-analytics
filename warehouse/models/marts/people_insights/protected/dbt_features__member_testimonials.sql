WITH appointments AS (

  SELECT * FROM {{ ref('stg_app__appointments') }}

),

testimonials AS (

  SELECT * FROM {{ ref('stg_app__testimonials') }}

),

development_topics AS (

  SELECT * FROM {{ ref('stg_app__development_topics') }}

)


SELECT DISTINCT
  {{ dbt_utils.surrogate_key(['member_id', 'appointment_id', 'ut.testimonial_id']) }} as unique_id,
  a.member_id,
  ut.testimonial_id AS associated_record_id,
  'Appointment' AS associated_record_type,
  a.starts_at AS feature_collected_at,
  'member_testimonial' AS feature_key,
  OBJECT_CONSTRUCT('translation_key', dt.translation_key) AS classification,
  OBJECT_CONSTRUCT('development_topic_key', dt.translation_key,'testimonial', ut.text, 'development_topic', dt.name ) AS feature_attributes,
  'testimonial' AS feature_type
FROM appointments AS a
INNER JOIN testimonials AS ut
  ON ut.assessment_id = a.post_session_member_assessment_id
INNER JOIN development_topics AS dt
  ON dt.development_topic_id = a.development_topic_id