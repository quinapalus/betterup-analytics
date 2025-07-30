WITH constructs AS (

  SELECT * FROM {{ source('wkfw', 'constructs') }}

)


SELECT
  id AS construct_id,
  mean,
  standard_deviation,
  {{ sanitize_i18n_field('description') }}
  {{ sanitize_i18n_field('title') }}
  {{ load_timestamp('created_at') }},
  {{ load_timestamp('updated_at') }}
FROM constructs
