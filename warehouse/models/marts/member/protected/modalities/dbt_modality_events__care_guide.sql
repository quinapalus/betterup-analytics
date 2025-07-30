{%- set modality = 'care_guide' -%}

{%- set modality_events = [
  'sent care_guide_message'
  ]
  -%}

-- To configure a modality it's only necessary to modify the parameters above,
-- not the template logic below

{% for event in modality_events -%}

  SELECT
    '{{ modality }}' AS modality,
    '{{ event }}' AS event_name
  {% if not loop.last %} UNION ALL {% endif %}

{%- endfor -%}
