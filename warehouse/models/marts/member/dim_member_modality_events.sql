{{
  config(
    tags=["eu"]
  )
}}

{%- set modalities = [
  'dbt_modality_events__care_coaching',
  'dbt_modality_events__care_guide',
  'dbt_modality_events__content',
  'dbt_modality_events__group_coaching'
  ]
  -%}

{%- for modality in modalities -%}

SELECT
  {{ dbt_utils.surrogate_key(['modality', 'event_name']) }} AS primary_key,
  modality,
  event_name
FROM {{ ref(modality) }}
{% if not loop.last %} UNION ALL {% endif %}
{%- endfor -%}
