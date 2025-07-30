WITH sessions AS (

  SELECT * FROM {{ref('stg_app__sessions')}}

),

timeslots AS (

  SELECT * FROM {{ref('dbt_timeslots')}}

),

slots_and_sessions AS (

  SELECT
    slots.coach_id,
    slots.timeslot_id,
    slots.starts_at AS slot_starts_at,
    slots.ends_at AS slot_ends_at,
    sess.starts_at AS sess_starts_at,
    sess.ends_at sess_ends_at,
    row_number() over (partition BY slots.timeslot_id ORDER BY sess.starts_at)
        AS nth_session_in_slot
    FROM (
      SELECT *
      FROM timeslots
      WHERE starts_at > current_timestamp
        AND starts_at <= dateadd('day', 60, current_timestamp)
      ) slots
    LEFT JOIN (
      SELECT *
      FROM sessions
      WHERE starts_at > current_timestamp
        AND starts_at <= dateadd('day', 60, current_timestamp)
        AND canceled_at is NULL
      ) sess
        ON sess.timeslot_id = slots.timeslot_id

),

-- blocks starting at the beginning of the timeslot
start_blocks AS (

  SELECT
    coach_id,
    timeslot_id,
    slot_starts_at AS block_starts_at,
    COALESCE(MIN(sess_starts_at), slot_ends_at) AS block_ends_at
  FROM slots_and_sessions
  GROUP BY
    coach_id,
    timeslot_id,
    slot_starts_at,
    slot_ends_at

),

-- blocks ending at the end of the timeslot
end_blocks AS (

  SELECT
    coach_id,
    timeslot_id,
    COALESCE(MAX(sess_ends_at), slot_starts_at) AS block_starts_at,
    slot_ends_at AS block_ends_at
  FROM slots_and_sessions
  GROUP BY
    coach_id,
    timeslot_id,
    slot_starts_at,
    slot_ends_at

),

-- blocks between sessions
middle_blocks AS (

  SELECT
    sess_1.coach_id,
    sess_1.timeslot_id,
    sess_1.sess_ends_at AS block_starts_at,
    sess_2.sess_starts_at AS block_ends_at
  FROM slots_and_sessions sess_1
    JOIN slots_and_sessions sess_2
      ON sess_1.timeslot_id = sess_2.timeslot_id
        AND sess_2.nth_session_in_slot = sess_1.nth_session_in_slot + 1
  WHERE sess_1.sess_starts_at IS NOT NULL
    AND sess_2.sess_starts_at IS NOT NULL

)


, final as (

-- UNION (*not* UNION ALL) should give us the distinct available blocks
SELECT *,
DATEDIFF( 'minute', block_starts_at, block_ends_at) as block_duration_minutes
FROM (
  SELECT * FROM start_blocks
  UNION
  SELECT * FROM end_blocks
  UNION
  SELECT * FROM middle_blocks
  ) all_blocks
-- filtering only blocks which are at least 30 minutes
WHERE DATEDIFF( 'minute', block_starts_at, block_ends_at) > 0
ORDER BY coach_id, block_starts_at

)

select
    --Band-aid primary key -- seems like we probably don't want a single timeslot counted twice, but that's
    --how it's built at the moment
    {{ dbt_utils.surrogate_key(['timeslot_id', 'block_starts_at']) }} as timeslot_block_start_key,
    *
from final
