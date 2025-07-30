--this fact table takes all of the customer journey events, unions them together and creates a surrogate key.
{{
  config(
    materialized='table'
  )
}}

{%- set events = [
  'event_lead_made_inquiry',
  'event_lead_reached_mql',
  'event_lead_reached_sal',
  'event_lead_reached_fm',
  'event_opportunity_created',
  'event_opportunity_reached_stage_1',
  'event_opportunity_reached_stage_2',
  'event_opportunity_reached_stage_3',
  'event_opportunity_reached_stage_4',
  'event_opportunity_reached_stage_5',
  'event_opportunity_reached_stage_6',
  'event_opportunity_closed_lost',
  'event_opportunity_closed_won'
  ]
  -%}
{%- for event in events -%}
SELECT
  {{ dbt_utils.surrogate_key(['sfdc_person_id','sfdc_opportunity_id','sfdc_account_id','associated_record_id', 'event_name','event_at']) }} AS primary_key,
  --logic to generate management plan targets surrogate key
  concat(coalesce(case when attributes:management_targets_target = 'Revenue' then '' else coalesce(attributes:management_targets_super_channel,attributes:super_channel) end,''),
                coalesce(case when attributes:management_targets_target = 'Revenue' then ''
                              else attributes:opportunity_owner_region end,''),
                coalesce(attributes:management_targets_geo,'')) as management_target_key,
  *
FROM {{ ref(event) }}
{% if not loop.last %} UNION ALL {% endif %}
{%- endfor -%}
