{%- set funnel_type = 'foundations' -%}

{%- set funnel_events = [
  'invited foundations_product',
  'activated foundations_user',
  'completed converged_onboarding'
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
