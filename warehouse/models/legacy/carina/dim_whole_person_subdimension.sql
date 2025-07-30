{{
  config(
    tags=['classification.c3_confidential','eu']
  )
}}

{% set wpm_domain_sort_order_dict = {
    "Mindset" : 1,
    "Thriving" : 2,
    "Inspiring" : 3,
    "Outcome": 4} -%}

with whole_person_subdimensions as (

  select * from {{ref('dbt_whole_person_subdimensions')}}

)


select
  whole_person_subdimension_key,
  whole_person_model_version,
  assessment_subdimension_key,
  subdimension_name,
  subdimension_description,
  -- fields for role visiblity
  visible_to_member as subdimension_is_visible_to_member,
  visible_to_partner as subdimension_is_visible_to_partner,
  included_in_partner_360_reporting as subdimension_is_included_in_partner_360_reporting,
  -- denormalized model hierarchy
  subdimension_dimension as wpm_dimension_raw,
  subdimension_domain as wpm_domain_raw,
  subdimension_category as wpm_category,
  iff(wpm_category = 'Behavior', wpm_domain_raw, wpm_category) as wpm_domain,
  iff(wpm_category = 'Behavior', wpm_dimension_raw, subdimension_name) as wpm_dimension,

  --iterating over key value pairs in wpm_domain_sort_order_dict at top of model.
  --for each key value pair: case when wpm_domain_sort_order_dict = key then return value 
  case
    {% for key, value in wpm_domain_sort_order_dict.items() %}
    when wpm_domain = '{{ key }}'
      then '{{ value }}' 
    {% endfor %}
  else null end as wpm_domain_sort_order

from whole_person_subdimensions
