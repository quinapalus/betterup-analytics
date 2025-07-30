{{
  config(
    tags=['eu'],
    materialized='table'
  )
}}

WITH reporting_groups AS (

  SELECT * FROM  {{ ref('int_app__reporting_groups') }}

),

reporting_group_records AS (

  SELECT * FROM  {{ ref('stg_app__reporting_group_records') }}

),

reporting_group_organizations AS (

  SELECT * FROM  {{ ref('int_app__reporting_group_organizations') }}

),

product_subscription_assignments AS (

  SELECT * FROM  {{ ref('int_app__product_subscription_assignments') }}
  WHERE starts_at <= CURRENT_TIMESTAMP

),

product_subscriptions AS (

  SELECT * FROM  {{ ref('stg_app__product_subscriptions') }}

),

track_assignments AS (

  SELECT * FROM  {{ ref('stg_app__track_assignments') }}
  -- making sure hidden track assignments don't show up
  WHERE NOT is_hidden

),

tracks AS (

  SELECT * FROM  {{ ref('dim_tracks') }}

),


unioned_assignments AS (

  SELECT
    'ProductSubscription' AS associated_record_type,
    ps.product_subscription_id AS associated_record_id,
    ps.organization_id,
    psa.product_subscription_assignment_id AS associated_assignment_id,
    psa.member_id,
    psa.starts_at,
    psa.ended_at,
    psa.updated_at,
    ps.name
  FROM product_subscription_assignments AS psa
  INNER JOIN product_subscriptions AS ps
    ON psa.product_subscription_id = ps.product_subscription_id

  UNION ALL

  SELECT
    'Track' AS associated_record_type,
    t.track_id AS associated_record_id,
    t.organization_id,
    ta.track_assignment_id AS associated_assignment_id,
    ta.member_id,
    ta.created_at AS starts_at,
    ta.ended_at,
    ta.updated_at,
    t.name
  FROM track_assignments AS ta
  INNER JOIN tracks AS t
    ON ta.track_id = t.track_id

)


SELECT distinct
  {{ dbt_utils.surrogate_key(['rg.reporting_group_id', 'rgr.associated_record_type', 'a.associated_assignment_id']) }} AS primary_key,
  rg.reporting_group_id,
  rg.name AS reporting_group_name,
  rgr.associated_record_type,
  rgr.associated_record_id,
  a.associated_assignment_id,
  a.member_id,
  a.starts_at,
  a.ended_at,
  a.updated_at,
  a.name AS track_or_product_subscription_name,
  -- a member is considered open if any one of their assignments is open, that is, not ended
  BOOLOR_AGG(a.ended_at IS NULL) OVER (PARTITION BY a.member_id, rg.reporting_group_id) AS member_is_open
FROM reporting_groups AS rg
INNER JOIN reporting_group_organizations AS rgo
  ON rg.reporting_group_id = rgo.reporting_group_id
INNER JOIN reporting_group_records AS rgr
  ON rg.reporting_group_id = rgr.reporting_group_id
INNER JOIN unioned_assignments AS a
  ON rgr.associated_record_type = a.associated_record_type AND
     rgr.associated_record_id = a.associated_record_id AND
     rgo.organization_id = a.organization_id
