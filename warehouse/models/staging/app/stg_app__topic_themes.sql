with topic_themes as (

  select * from {{ ref('topic_themes') }}

)

select
  --ids
  id as topic_theme_id,
  translation_key,

  --categorical and text
  {{ sanitize_i18n_field('name') }}
  --alias name is referenced below because sanitize_i18n_field also creates a column called name.
  --need to make sure that name deprecated references the name column in the source table.
  topic_themes.name as name_deprecated,
  {{ sanitize_i18n_field('collaborator_description') }}
  {{ sanitize_i18n_field('description') }}
  --alias name is referenced below because sanitize_i18n_field also creates a column called description.
  --need to make sure that description deprecated references the name column in the source table.
  topic_themes.description as description_deprecated,
  external_resources,
  image_url,
  topic_version,

  --numeric
  resources_count,
  
  --dates and timestamps
  {{ load_timestamp('created_at') }},
  {{ load_timestamp('updated_at') }}
  
from topic_themes
