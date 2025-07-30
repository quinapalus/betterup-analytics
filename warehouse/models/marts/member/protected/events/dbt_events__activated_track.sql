WITH users AS (

  SELECT * FROM  {{ ref('int_app__users') }}

),

track_assignments AS (

  SELECT * FROM {{ ref('stg_app__track_assignments') }}
  WHERE NOT is_hidden

),

track_assignments_enriched AS (

  SELECT
    ta.track_assignment_id,
    ta.member_id,
    ta.track_id,
    ta.created_at,
    ta.ended_at,
    ROW_NUMBER() OVER (PARTITION BY ta.member_id, ta.track_id ORDER BY ta.created_at) AS sequence,
    COUNT(*) OVER (PARTITION BY ta.member_id, ta.track_id) AS count,
    MIN(u.confirmed_at) OVER (PARTITION BY ta.member_id, ta.track_id) AS member_confirmed_at,
    -- a member is confirmed on the track if they confirmed within any
    -- associated track_assignment
    BOOLOR_AGG(u.confirmed_at IS NOT NULL) OVER (PARTITION BY ta.member_id, ta.track_id) AS member_is_confirmed,
    -- a member is considered open if any one of their assignments is open, that is, not ended
    BOOLOR_AGG(ta.ended_at IS NULL) OVER (PARTITION BY ta.member_id, ta.track_id) AS member_is_open
  FROM track_assignments AS ta
  LEFT OUTER JOIN users AS u
    -- join in any confirmation events that happened prior to ta.ended_at
    ON u.confirmed_at IS NOT NULL AND ta.member_id = u.user_id AND
       (ta.ended_at IS NULL OR u.confirmed_at < ta.ended_at)

),

final AS (

    SELECT
      member_id,
      -- if member activated prior to track invite, mark as activated on invite
      GREATEST(member_confirmed_at, created_at) AS event_at,
      'activated' AS event_action,
      'track' AS event_object,
      event_action || ' ' || event_object AS event_action_and_object,
      'TrackAssignment' AS associated_record_type,
      track_assignment_id AS associated_record_id,
      OBJECT_CONSTRUCT('track_assignment_count', count, 'member_is_open', member_is_open, 'member_activated_prior_to_invitation', member_confirmed_at < created_at) AS attributes
    FROM track_assignments_enriched
    WHERE member_is_confirmed AND sequence = 1
)

SELECT
    final.*
FROM final
