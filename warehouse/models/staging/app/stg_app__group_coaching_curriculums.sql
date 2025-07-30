{{
  config(
    tags=["eu"]
  )
}}

with group_coaching_curriculums as (

  select * from {{ source('app','group_coaching_curriculums') }}

),

renamed as (

  select
    id as group_coaching_curriculum_id,
    uuid as group_coaching_curriculum_uuid,
    {{ sanitize_i18n_field('title') }}
    {{ sanitize_i18n_field('description') }}
    learn_more_resource_id,
    intervention_type,
    {{ load_timestamp('created_at') }},
    {{ load_timestamp('updated_at') }}
  from group_coaching_curriculums

)

select * from renamed
