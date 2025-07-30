WITH users AS (

  SELECT * FROM {{ref('int_app__users')}}

)

SELECT
  u.user_id,
  u.time_zone,
  u.tz_iana,
  u.country_code,
  u.country_name,
  u.subregion_m49,
  u.geo,
  u.confirmed_at,
  u.confirmation_sent_at,
  u.completed_member_onboarding_at,
  u.deactivated_at,
  u.next_session_id,
  u.inviter_id,
  u.language,
  u.coaching_language,
  u.roles,
  u.title,
  u.organization_id,
  u.manager_id,
  u.created_at,
  u.updated_at
FROM users AS u