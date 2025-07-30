with development_topics as (

  select * from {{ source('app', 'development_topics') }}

)

select

--ids
  id as development_topic_id,
  --this macro creates two columns: name_i18n and name. "name" has the parsed json version
  {{ sanitize_i18n_field('name') }}
  translation_key,
  topic_theme_id,

--categoricals
  topic_version,

--booleans
  iff(coach_selectable,true,false) as is_coach_selectable,
  iff(member_selectable,true,false) as is_member_selectable,

--dates and timestamps
  {{ load_timestamp('created_at') }},
  {{ load_timestamp('updated_at') }}

FROM development_topics
