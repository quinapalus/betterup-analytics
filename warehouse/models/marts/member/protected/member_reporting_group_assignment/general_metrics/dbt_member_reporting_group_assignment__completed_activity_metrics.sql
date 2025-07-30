WITH completed_activity AS (

  SELECT * FROM {{ref('dbt_events__completed_activity')}}

),

reporting_group_assignments AS (

  SELECT * FROM {{ref('dim_reporting_group_assignments')}}

),

final as (

  SELECT
    {{ dbt_utils.surrogate_key(['rga.member_id', 'rga.reporting_group_id', 'rga.associated_assignment_id']) }} as member_reporting_group_assignment_key,
    rga.member_id,
    rga.reporting_group_id,
    rga.associated_assignment_id,
    COUNT(*) AS completed_activity_count,
    SUM(IFF(ca.attributes:"resource_id"::varchar IS NOT NULL, 1, 0))
      AS completed_resource_count
  FROM reporting_group_assignments AS rga
  INNER JOIN completed_activity AS ca
      ON rga.member_id = ca.member_id AND
        ca.event_at >= rga.starts_at AND
        (rga.ended_at IS NULL OR ca.event_at < rga.ended_at)
  GROUP BY member_reporting_group_assignment_key, rga.member_id, rga.reporting_group_id, rga.associated_assignment_id

)

select * from final
