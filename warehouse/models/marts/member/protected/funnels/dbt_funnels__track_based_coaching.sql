{%- set funnel_type = 'track_based_coaching' -%}

{%- set funnel_events = [
  'invited track',
  'activated track',
  'completed track_onboarding',
  'completed onboarding_assessment_track'
  ]
  -%}

-- To configure a funnel it's only necessary to modify the parameters above,
-- not the template logic below
with unioned as (
{% for event in funnel_events -%}

  SELECT
    '{{ funnel_type }}' AS funnel_type,
    '{{ event }}' AS event_name,
    {{ loop.index }} AS funnel_stage
  {% if not loop.last %} UNION ALL {% endif %}

{%- endfor -%}
),

final as (
  select
    {{ dbt_utils.surrogate_key(['funnel_type', 'event_name', 'funnel_stage']) }} as _unique,
    *
  from unioned
)

select * from final