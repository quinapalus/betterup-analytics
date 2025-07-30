{%- set modality = 'content' -%}

{%- set modality_events = [
  'viewed resource'
  ]
  -%}

-- To configure a modality it's only necessary to modify the parameters above,
-- not the template logic below

{% for event in modality_events -%}

  SELECT
    '{{ modality }}' AS modality,
    '{{ event }}' AS event_name,
    --Adding _grain column for uniqueness + not null test coverage on this int model. 
    --Primary key is added downstream in dim_member_modality_events
    {{ dbt_utils.surrogate_key(['modality', 'event_name']) }} as _grain
  {% if not loop.last %} UNION ALL {% endif %}

{%- endfor -%}
