{%- set funnels = [
  'dbt_funnels__care',
  'dbt_funnels__foundations',
  'dbt_funnels__group_coaching',
  'dbt_funnels__track_based_coaching',
  ]
  -%}

{%- for funnel in funnels -%}

SELECT 
        {{ dbt_utils.surrogate_key(['funnel_type', 'event_name', 'funnel_stage']) }} AS primary_key
     ,  funnel_type
     , event_name
     , funnel_stage
FROM {{ ref(funnel) }}
{% if not loop.last %} UNION ALL {% endif %}

{%- endfor -%}
