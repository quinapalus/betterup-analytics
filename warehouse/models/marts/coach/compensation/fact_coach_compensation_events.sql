--this fact table takes all of the coach comp events and unions them all together. No season logic. 
{{
  config(
    tags=['classification.c3_confidential'],
    materialized='table'
  )
}}

{%- set events = [
  'dbt_events__completed_billable_event',
  'dbt_events__completed_coaching_session',
  'dbt_events__submitted_post_session_assessment'
  ]
  -%}
{%- for event in events -%}
SELECT
  {{ dbt_utils.surrogate_key(['member_id','coach_id','associated_record_id', 'associated_record_type', 'event_action', 'event_object']) }} AS primary_key,
  *
FROM {{ ref(event) }}
{% if not loop.last %} UNION ALL {% endif %}
{%- endfor -%}