WITH track_assignments AS (

  SELECT * FROM {{ref('dbt_track_assignments')}}

),

enrollment_attributes AS (

  SELECT
    -- consolidate attributes from multiple track_assignments per member-track
    -- combination, using attributes from the open track_assignment, or most
    -- recently closed track_assignment if no open assignments exist.
    member_id,
    track_id,
    is_primary_coaching_enabled,
    is_on_demand_coaching_enabled,
    is_extended_network_coaching_enabled,
    ROW_NUMBER() OVER (PARTITION BY member_id, track_id
      ORDER BY member_id, track_id, ended_at DESC NULLS FIRST) as sequence
  FROM track_assignments
  QUALIFY sequence = 1

)


SELECT
  {{ dbt_utils.surrogate_key(['ta.member_id', 'ta.track_id']) }}  AS track_enrollment_id,
  ta.member_id,
  ta.track_id,
  MIN(ta.created_at) AS invited_at,
  CASE
    -- return NULL if *any* track_assignment is still open
    WHEN {{bool_or()}}(ta.ended_at IS NULL) THEN NULL
    ELSE MAX(ta.ended_at)
  END AS ended_at,
  -- return available minutes based on open track_assignments
  SUM(CASE WHEN ta.ended_at IS NULL THEN ta.minutes_limit - ta.minutes_used END) / 60.0
    AS coaching_hours_remaining,
  ea.is_primary_coaching_enabled,
  ea.is_on_demand_coaching_enabled,
  ea.is_extended_network_coaching_enabled,
  COUNT(*) AS track_assignments_count,
  {{bool_and()}}(ta.is_hidden = true) AS is_hidden
FROM track_assignments AS ta
INNER JOIN enrollment_attributes AS ea
  ON ta.member_id = ea.member_id AND
     ta.track_id = ea.track_id
GROUP BY ta.member_id, ta.track_id, ea.is_primary_coaching_enabled,
         ea.is_on_demand_coaching_enabled, ea.is_extended_network_coaching_enabled
