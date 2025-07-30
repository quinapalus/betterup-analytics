{%- set modality = 'group_coaching' -%}

{%- set modality_events = [
  'completed group_coaching_appointment',
  ]
  -%}

-- To configure a modality it's only necessary to modify the parameters above,
-- not the template logic below

{% for event in modality_events -%}

  SELECT
    -- Preparing 'Event Name' for surrogate key by replacing spaces with underscores
    {% set surrogate_key_vals %}
      CONCAT('{{ event.replace(' ','_') }}','{{ modality }})')
    {% endset %}
    -- Surrogate Key of Modality + Event for future resilience, Primary Key functionality
    {{ dbt_utils.surrogate_key([surrogate_key_vals]) }} AS modality_event_name_id,
    '{{ modality }}' AS modality,
    '{{ event }}' AS event_name
  {% if not loop.last %} UNION ALL {% endif %}

{%- endfor -%}