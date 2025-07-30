WITH member_engagement_by_day AS (

  SELECT * FROM {{ref('dei_member_engagement_by_day')}}

),

members AS (

  SELECT * FROM {{ref('dei_members')}}

),

dim_member AS (

  SELECT * FROM {{ref('dim_member')}}

),

dim_account AS (

  SELECT * FROM {{ref('dim_account')}}

),

track_enrollments AS (

  SELECT * FROM {{ref('dbt_track_enrollments')}}

)


SELECT
  {{ date_key('med.date_day') }} AS date_key,
  {{ member_key('med.member_id') }} AS member_key,
  {{ member_deployment_key ('med.member_id', 'med.track_id')}} AS member_deployment_key,
  {{ account_key('med.organization_id', 'med.sfdc_account_id') }} AS account_key,
  {{ contract_key('med.contract_id') }} AS contract_key,
  {{ deployment_key('med.track_id') }} AS deployment_key,
  ROW_NUMBER() OVER (PARTITION BY med.member_id, med.track_id ORDER BY med.date_day) - 1
    AS days_since_invite,
  DATEDIFF('DAY', med.date_day, te.ended_at) AS days_until_end_date_on_deployment, -- if member closed, use ended_at instead of ends_on
  m.activated_at IS NOT NULL AND med.date_day >= DATE_TRUNC('DAY', m.activated_at) AS is_activated,
  med.has_upcoming AS has_upcoming_session,
  med.primary_coach_id AS app_primary_coach_id,
  med.is_matched_with_primary_coach
FROM member_engagement_by_day AS med
INNER JOIN members AS m
  ON med.member_id = m.member_id
INNER JOIN track_enrollments AS te
  ON med.member_id = te.member_id
  AND med.track_id = te.track_id
WHERE
  -- ensure foreign keys are present in dimension tables
  {{member_key('med.member_id')}} IN (SELECT member_key FROM dim_member) AND
  {{account_key('med.organization_id', 'med.sfdc_account_id')}} IN (SELECT account_key FROM dim_account)
