WITH dim_date AS (
  SELECT * FROM {{ref('dim_date')}}
),

track_assignments AS (
  SELECT * FROM {{ref('stg_app__track_assignments')}}
),

users AS (
  SELECT * FROM {{ref('int_app__users')}}
),

join_track_assignments AS (
  SELECT ta.*,
    CASE
    -- If member is activated prior to track_assignment creation, mark track_assignment as activated on creation:
    WHEN u.confirmed_at < ta.created_at THEN ta.created_at
    -- If track_assignment is open, or member activated prior to track_assignment ended, use date member activated (if any):
    WHEN ta.ended_at IS NULL OR u.confirmed_at < ta.ended_at THEN u.confirmed_at
    -- In case where member activated after track_assignment ended, track_assignment.activated_at is NULL:
    ELSE NULL
    END AS activated_at,

    -- lead_activated_at
    CASE
    -- If member is activated prior to track_assignment creation, mark track_assignment as activated on creation:
    WHEN u.lead_confirmed_at < ta.created_at THEN ta.created_at
    -- If track_assignment is open, or member activated prior to track_assignment ended, use date member activated (if any):
    WHEN ta.ended_at IS NULL OR u.lead_confirmed_at < ta.ended_at THEN u.lead_confirmed_at
    -- In case where member activated after track_assignment ended, track_assignment.activated_at is NULL:
    ELSE NULL
    END AS lead_activated_at,

    -- care_activated_at
    CASE
    -- If member is activated prior to track_assignment creation, mark track_assignment as activated on creation:
    WHEN u.care_confirmed_at < ta.created_at THEN ta.created_at
    -- If track_assignment is open, or member activated prior to track_assignment ended, use date member activated (if any):
    WHEN ta.ended_at IS NULL OR u.care_confirmed_at < ta.ended_at THEN u.care_confirmed_at
    -- In case where member activated after track_assignment ended, track_assignment.activated_at is NULL:
    ELSE NULL
    END AS care_activated_at,

    -- For members that have multiple track_assignments for a given track, find the first invite date:
    CASE WHEN NOT ta.is_hidden THEN MIN(ta.created_at) OVER (PARTITION BY ta.member_id, ta.track_id, ta.is_hidden) END AS member_first_invited_to_track_at
  FROM track_assignments AS ta
  INNER JOIN users AS u
    ON ta.member_id = u.user_id
),

joined as (
SELECT
  c.date_key,
  c.date,
  c.calendar_year_month,
  c.is_current_fiscal_quarter,
  c.is_previous_fiscal_quarter,
  c.date = LAST_DAY(c.date) AS is_last_day_of_month,
  t.track_assignment_id,
  t.track_id,
  t.member_id,
  t.member_first_invited_to_track_at,
  t.created_at,
  t.activated_at,
  CASE
    WHEN MAX(c.date) OVER(PARTITION BY t.track_assignment_id) > CURRENT_DATE() THEN DATEDIFF(day, t.activated_at, CURRENT_DATE())
    ELSE DATEDIFF(day, t.activated_at, MAX(c.date) OVER(PARTITION BY t.track_assignment_id))
  END AS total_days_in_tenure_since_activation,
  t.lead_activated_at,
  t.care_activated_at,
  t.is_primary_coaching_enabled,
  t.is_on_demand_coaching_enabled,
  t.is_extended_network_coaching_enabled,
  t.is_care_coaching_enabled,
  MAX(activated_at) OVER(PARTITION BY date, member_id) AS max_activated_date,
  c.date >= date_trunc('day', t.activated_at) AS member_was_activated_on_track,
  IFF(member_was_activated_on_track, CEIL(DATEDIFF('second', t.activated_at, c.date) / 86400.0), NULL) AS days_since_activation,
  IFF(member_was_activated_on_track, CEIL(days_since_activation / 30.0), NULL) AS months_since_activation
FROM dim_date AS c
INNER JOIN join_track_assignments AS t
  ON c.date >= date_trunc('day', t.created_at) AND
     c.date < COALESCE(t.ended_at, DATEADD(DAY, 365, CURRENT_DATE()))
),
 
final as (
  SELECT 
  {{ dbt_utils.surrogate_key(['member_id', 'date_key']) }} as member_track_calendar_id,
  * 
  FROM joined
  WHERE (activated_at = max_activated_date OR activated_at IS NULL) -- logic to select the latest track a member was active on for a given day
  QUALIFY ROW_NUMBER() OVER (PARTITION BY date, member_id ORDER BY created_at desc) = 1 -- logic that limits to unique record per member per day
)

select *
from final
