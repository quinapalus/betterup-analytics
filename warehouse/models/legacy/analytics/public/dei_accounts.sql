WITH organizations AS (

  SELECT * FROM {{ref('stg_app__organizations')}}

),

tracks AS (

  SELECT * FROM {{ref('dim_tracks')}}

),

billable_sessions AS (

  SELECT * FROM {{ref('dbt_billable_sessions')}}

),

sfdc_accounts AS (

  SELECT * FROM {{ ref('stg_sfdc__accounts') }}

),

sfdc_users AS (

  SELECT * FROM {{ ref('stg_sfdc__users') }}

),

external_tracks AS (

  SELECT * FROM tracks 
  WHERE is_external and engaged_member_count is not null --this logic was in dei_tracks which this model used to reference

),

external_sessions AS (

  SELECT
    t.organization_id,
    MIN(bs.event_at) AS first_paid_session_at,
    MAX(bs.event_at) AS last_paid_session_at
  FROM billable_sessions AS bs
  INNER JOIN external_tracks AS t
    ON bs.track_id = t.track_id
  GROUP BY t.organization_id

)


SELECT
  o.organization_id,
  o.name AS organization_name,
  o.sfdc_account_id,
  sa.account_name AS sfdc_account_name,
  owner.first_name || ' ' || owner.last_name AS sfdc_account_owner,
  COALESCE(csm.first_name || ' ' || csm.last_name, 'N/A') AS sfdc_account_csm,
  sa.industry,
  sa.company_size,
  sa.account_segment,
  es.first_paid_session_at,
  es.last_paid_session_at,
  CASE
    WHEN es.last_paid_session_at < dateadd('month', -6, current_timestamp) THEN 'inactive'
    ELSE 'active'
  END AS account_status
FROM organizations AS o
LEFT OUTER JOIN external_sessions AS es
  ON o.organization_id = es.organization_id
INNER JOIN sfdc_accounts AS sa
  ON o.sfdc_account_id = sa.sfdc_account_id
-- Denormalize Owner and CSM name using sfdc_users model
-- All sfdc_accounts will have an Owner, while CSM is only
-- populated for post-sales accounts (therefore use LEFT JOIN)
INNER JOIN sfdc_users AS owner
  ON sa.account_owner_id = owner.sfdc_user_id
LEFT OUTER JOIN sfdc_users AS csm
  ON sa.account_csm_id = csm.sfdc_user_id
WHERE o.organization_id IN (SELECT DISTINCT organization_id FROM external_tracks)
