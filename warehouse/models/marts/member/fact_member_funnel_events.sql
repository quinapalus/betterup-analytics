{{
  config(
    tags=['classification.c3_confidential','eu'],
  )
}}

WITH member_events AS (

  SELECT
    *
  FROM  {{ ref('fact_member_events') }}

),

member_funnels AS (

  SELECT * FROM  {{ ref('dim_member_funnels') }}

),

intermediate AS (

  SELECT
    {{ dbt_utils.surrogate_key(['e.member_id', 'e.event_name', 'e.event_at', 'f.funnel_type']) }} AS primary_key,
    f.funnel_type,
    e.event_name,
    f.funnel_stage,
    e.member_id,
    e.event_at,
    e.event_action,
    e.event_object
  FROM member_events AS e
  INNER JOIN member_funnels AS f
    ON e.event_name = f.event_name

)


-- Limiting results to members that have completed the first step in a given funnel
SELECT DISTINCT i.* FROM intermediate as i
INNER JOIN intermediate as i1
  ON i.member_id = i1.member_id AND
     i1.funnel_stage = 1 AND
     i.funnel_type = i1.funnel_type