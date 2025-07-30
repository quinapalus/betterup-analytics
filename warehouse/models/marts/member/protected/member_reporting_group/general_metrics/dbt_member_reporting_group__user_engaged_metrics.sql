WITH user_engaged AS (

  SELECT * FROM {{ref('dbt_events__user_engaged')}}
),

reporting_group_assignments AS (

  SELECT * FROM {{ref('dim_reporting_group_assignments')}}

)


SELECT
  rga.member_id,
  rga.reporting_group_id,
  MAX(ue.event_at) AS last_engagement_at
FROM reporting_group_assignments AS rga
INNER JOIN user_engaged AS ue
    ON rga.member_id = ue.member_id AND
       ue.event_at >= rga.starts_at AND
       (rga.ended_at IS NULL OR ue.event_at < rga.ended_at)
GROUP BY rga.member_id, rga.reporting_group_id