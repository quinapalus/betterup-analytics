WITH assessment_configurations AS (

  SELECT * FROM {{ source('assessment', 'assessment_configurations') }}

)

SELECT
  id as assessment_configuration_id,
  uuid as assessment_configuration_uuid,
  TITLE_I18N,
  parse_json(TITLE_I18N):"en"::varchar as title_en,
  TOKEN,
  SHOW_REPORT_PAGE_ON_SUBMIT,
  description,
  {{ load_timestamp('created_at') }},
  {{ load_timestamp('updated_at') }}
FROM assessment_configurations

