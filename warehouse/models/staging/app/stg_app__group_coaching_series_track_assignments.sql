{{
  config(
    tags=['classification.c3_confidential']
  )
}}

WITH group_coaching_series_track_assignments AS (

  SELECT * FROM {{ source('app', 'group_coaching_series_track_assignments') }}

),

group_coaching_series AS(

  SELECT * FROM {{ source('app', 'group_coaching_series') }}

)


SELECT
  sta.id AS group_coaching_series_track_assignment_id,
  sta.group_coaching_series_id,
  sta.track_assignment_id,
  sta.source_id,
  sta.source_type,
  -- Rationalize created_at timestamp for series_track_assignment records that were created during initial backpopulation
  IFF( sta.created_at > cs.registration_end, cs.registration_start::timestamp_ntz, sta.created_at::timestamp_ntz) AS created_at,
  sta.{{ load_timestamp('updated_at') }},
  sta.{{ load_timestamp('revoked_at') }}
FROM group_coaching_series_track_assignments AS sta
INNER JOIN group_coaching_series AS cs
    ON sta.group_coaching_series_id = cs.id
