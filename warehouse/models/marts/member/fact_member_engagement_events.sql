{{
  config(
    tags=['classification.c3_confidential','eu'],
    materialized='table'
  )
}}

{%- set events = [
  'dbt_events__attended_group_coaching_appointment',
  'dbt_events__completed_activity',
  'dbt_events__completed_appointment',
  'dbt_events__engaged_objective',
  'dbt_events__engaged_resource',
  'dbt_events__registered_group_coaching',
  'dbt_events__selected_coach',
  'dbt_events__sent_care_guide_message',
  'dbt_events__sent_group_message',
  'dbt_events__sent_message',
  'dbt_events__submitted_assessment',
  'dbt_events__viewed_assessment',
  'dbt_events__viewed_insights'
  ]
  -%}

{%- for event in events -%}

SELECT
  {{ dbt_utils.surrogate_key(['member_id', 'associated_record_id', 'associated_record_type', 'event_action', 'event_object', 'event_at']) }} AS primary_key,
  member_id,
  event_at,
  event_action,
  event_object,
  event_action_and_object,
  associated_record_type,
  associated_record_id,
  attributes
FROM {{ ref(event) }}
{% if not loop.last %} UNION ALL {% endif %}
{%- endfor -%}