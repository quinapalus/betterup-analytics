WITH resources AS (

  SELECT * FROM {{ source('app', 'resources') }}

)

SELECT
  id AS resource_id,
  uuid AS resource_uuid,
  assessment_configuration_uuid,
  {{ sanitize_i18n_field('source') }}
  {{ sanitize_i18n_field('title') }}
  {{ sanitize_i18n_field('description') }}
  type,
  duration,
  NULLIF(language, '') AS language,
  retired,
  hide_from_recommendation_algorithm,
  recommended,
  publicly_available,
  url,
  audio_embed_url,
  {{ sanitize_i18n_field('content') }}
  content_type,
  content_modality,
  verb,
  cached_tag_list AS tag_list,
  creator_id,
  host,
  {{ sanitize_i18n_field('author') }}
  external_image_url,
  ingeniux_id,
  {{ load_timestamp('created_at') }},
  {{ load_timestamp('updated_at') }}
FROM resources
