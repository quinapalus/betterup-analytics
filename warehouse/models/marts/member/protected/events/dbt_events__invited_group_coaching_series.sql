WITH group_coaching_series_track_assignments AS (

  SELECT * FROM {{ ref('stg_app__group_coaching_series_track_assignments') }}

),

track_assignments AS (

  SELECT * FROM {{ ref('stg_app__track_assignments') }}

),

tracks AS (

  SELECT * FROM {{ ref('dim_tracks') }}

),

group_coaching_series AS (

  SELECT * FROM {{ ref('stg_app__group_coaching_series') }}

),

group_coaching_curriculums AS (

  SELECT * FROM {{ ref('stg_app__group_coaching_curriculums') }}

),

group_coaching_cohorts AS (

  SELECT * FROM {{ ref('stg_app__group_coaching_cohorts') }}

),

group_coaching_registrations AS (

  SELECT * FROM {{ ref('stg_app__group_coaching_registrations') }}

),

product_subscription_assignments AS (

  SELECT * FROM {{ ref('int_app__product_subscription_assignments') }}
  WHERE starts_at <= CURRENT_TIMESTAMP

),

product_subscriptions AS (

  SELECT * FROM {{ ref('stg_app__product_subscriptions') }}

),

products AS (

  SELECT * FROM {{ ref('stg_app__products') }}

),

join_products AS (

  SELECT
    psa.member_id,
    psa.product_subscription_assignment_id,
    psa.starts_at,
    COALESCE(psa.ended_at, psa.ends_at) AS ended_at
  FROM product_subscription_assignments AS psa
  INNER JOIN product_subscriptions AS ps
    ON psa.product_subscription_id = ps.product_subscription_id
  INNER JOIN products AS p
    ON ps.product_id = p.product_id
  WHERE
    p.coaching_circles OR
    p.workshops

),

join_registrations_cohorts AS (

  SELECT
    r.group_coaching_registration_id,
    r.user_id,
    r.created_at,
    c.group_coaching_series_id
  FROM group_coaching_registrations AS r
  INNER JOIN group_coaching_cohorts AS c
    ON r.group_coaching_cohort_id = c.group_coaching_cohort_id
  WHERE r.canceled_at IS NULL
  -- Members can register for multiple cohorts in the same series
  QUALIFY ROW_NUMBER() OVER (PARTITION BY r.user_id, c.group_coaching_series_id ORDER BY r.created_at) = 1

),

join_sta AS (

  SELECT DISTINCT
    sta.*,
    ta.member_id,
    cs.registration_start,
    cs.registration_end,
    cs.group_coaching_curriculum_id,
    t.name AS track_name,
    ta.ended_at
  FROM group_coaching_series_track_assignments AS sta
  INNER JOIN group_coaching_series AS cs
    ON sta.group_coaching_series_id = cs.group_coaching_series_id
  INNER JOIN track_assignments AS ta
    ON sta.track_assignment_id = ta.track_assignment_id
  INNER JOIN tracks AS t
    ON t.track_id = ta.track_id
  INNER JOIN join_products AS p
    ON ta.member_id = p.member_id
  WHERE
    p.starts_at <= cs.registration_end AND
    p.ended_at >= cs.registration_start

),

renamed as (
SELECT
  jsta.member_id,
  jsta.created_at AS event_at,
  'invited' AS event_action,
  c.intervention_type || '_series' AS event_object,
  event_action || ' ' || event_object AS event_action_and_object,
  'GroupCoachingSeriesTrackAssignment' AS associated_record_type,
  jsta.group_coaching_series_track_assignment_id AS associated_record_id,
  OBJECT_CONSTRUCT('group_coaching_series_id', jsta.group_coaching_series_id,
                   'registration_start', jsta.registration_start,
                   'registration_end', jsta.registration_end,
                   'intervention_type', c.intervention_type,
                   'group_coaching_curriculum_title', c.title,
                   'track_name', jsta.track_name,
                    'track_assignment_ended_at', jsta.ended_at
                  ) AS attributes
FROM join_sta AS jsta
INNER JOIN group_coaching_curriculums AS c
  ON jsta.group_coaching_curriculum_id = c.group_coaching_curriculum_id
LEFT JOIN join_registrations_cohorts AS jrc
  ON jsta.group_coaching_series_id = jrc.group_coaching_series_id AND
     jsta.member_id = jrc.user_id
WHERE
  jsta.revoked_at IS NULL OR
  jsta.revoked_at > jrc.created_at
),

final as (
  select
    --primary key
  {{ dbt_utils.surrogate_key(['member_id', 'event_at', 'event_action_and_object', 'associated_record_id'])}} as _unique,
  *
  from renamed
)

select * from final