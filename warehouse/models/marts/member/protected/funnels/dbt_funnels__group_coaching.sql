{%- set funnel_type = 'group_coaching' -%}

{%- set funnel_events = [
  'invited coaching_circles_product',
  'activated coaching_circles_user',
  'registered coaching_circle'
  ]
  -%}

-- To configure a funnel it's only necessary to modify the parameters above,
-- not the template logic below

{% for event in funnel_events -%}

  SELECT
    '{{ funnel_type }}' AS funnel_type,
    '{{ event }}' AS event_name,
    {{ loop.index }} AS funnel_stage
  {% if not loop.last %} UNION ALL {% endif %}

{%- endfor -%}
