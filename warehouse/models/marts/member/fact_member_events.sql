{{
  config(
    tags=['eu']
  )
}}

{%- set events = [
  'dbt_events__activated',
  'dbt_events__activated_lead_product',
  'dbt_events__activated_track',
  'dbt_events__assigned_program_journey',
  'dbt_events__attended_group_coaching_appointment',
  'dbt_events__completed_activity',
  'dbt_events__completed_appointment',
  'dbt_events__completed_care_modality_setup',
  'dbt_events__completed_onboarding_assessment',
  'dbt_events__completed_onboarding_assessment_track',
  'dbt_events__completed_primary_modality_setup',
  'dbt_events__completed_profile',
  'dbt_events__engaged_objective',
  'dbt_events__engaged_resource',
  'dbt_events__invited_group_coaching_series',
  'dbt_events__invited_track',
  'dbt_events__invited',
  'dbt_events__invited_lead_product',
  'dbt_events__onboarded_track',
  'dbt_events__onboarded',
  'dbt_events__registered_group_coaching',
  'dbt_events__scheduled_appointment',
  'dbt_events__selected_coach',
  'dbt_events__selected_focus_area',
  'dbt_events__sent_care_guide_message',
  'dbt_events__sent_group_message',
  'dbt_events__sent_message',
  'dbt_events__started_assessment',
  'dbt_events__submitted_assessment',
  'dbt_events__user_engaged',
  'dbt_events__viewed_assessment',
  'dbt_events__viewed_insights'
  ]
  -%}

{%- for event in events -%}

SELECT
  {{ dbt_utils.surrogate_key(['member_id', 'associated_record_id', 'associated_record_type', 'event_action', 'event_object', 'event_at']) }} AS primary_key,
  member_id,
  event_action || ' ' || event_object AS event_name,
  event_at,
  event_action,
  event_object,
  associated_record_type,
  associated_record_id,
  attributes,
  -- maintain deprecated interface:
  event_name AS event_action_and_object
FROM {{ ref(event) }}
WHERE event_at IS NOT NULL
{% if not loop.last %} UNION ALL {% endif %}

{%- endfor -%}
