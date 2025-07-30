WITH assessment_configurations AS (

  SELECT * FROM {{ source('wkfw', 'assessment_configurations') }}

)


SELECT
  id AS assessment_configuration_id,
  theme_key,
  {{ sanitize_i18n_field('description') }}
  {{ sanitize_i18n_field('subtitle') }}
  {{ sanitize_i18n_field('title') }}
  {{ load_timestamp('created_at') }},
  {{ load_timestamp('updated_at') }}
FROM assessment_configurations
