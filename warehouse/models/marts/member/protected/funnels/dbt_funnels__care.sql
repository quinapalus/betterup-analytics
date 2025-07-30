{%- set funnel_type = 'care' -%}

{%- set funnel_events = [
  'invited care_product',
  'activated care_user',
  'completed care_onboarding'
  ]
  -%}

-- To configure a funnel it's only necessary to modify the parameters above,
-- not the template logic below

{% for event in funnel_events -%}

  SELECT
    -- Preparing 'Event Name' for surrogate key by replacing spaces with underscores
    {% set surrogate_key_vals %}
      CONCAT('{{ event.replace(' ','_') }}','{{ funnel_type }})')
    {% endset %}
    -- Surrogate Key of funnel_type + Event for future resilience, Primary Key functionality
    {{ dbt_utils.surrogate_key([surrogate_key_vals]) }} AS funnel_type_event_name_id,
    '{{ funnel_type }}' AS funnel_type,
    '{{ event }}' AS event_name,
    {{ loop.index }} AS funnel_stage
  {% if not loop.last %} UNION ALL {% endif %}

{%- endfor -%}
