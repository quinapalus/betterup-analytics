WITH track_assignments AS (

  SELECT * FROM {{ ref('stg_app__track_assignments') }}
  WHERE NOT is_hidden

),

track_assignments_enriched AS (

  SELECT
    track_assignment_id,
    member_id,
    track_id,
    created_at,
    ended_at,
    ROW_NUMBER() OVER (PARTITION BY member_id, track_id ORDER BY created_at) AS sequence,
    COUNT(*) OVER (PARTITION BY member_id, track_id) AS count,
    -- a member is considered open if any one of their assignments is open, that is, not ended
    BOOLOR_AGG(ended_at IS NULL) OVER (PARTITION BY member_id, track_id) AS member_is_open
  FROM track_assignments

)


SELECT
  -- primary key
  {{ dbt_utils.surrogate_key(['member_id', 'track_assignment_id']) }} AS dbt_events__invited_track_id,

  member_id,
  created_at AS event_at,
  'invited' AS event_action,
  'track' AS event_object,
  event_action || ' ' || event_object AS event_action_and_object,
  'TrackAssignment' AS associated_record_type,
  track_assignment_id AS associated_record_id,
  OBJECT_CONSTRUCT('track_assignment_count', count, 'member_is_open', member_is_open) AS attributes
FROM track_assignments_enriched
WHERE sequence = 1
