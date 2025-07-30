WITH reporting_group_assignments AS (

  SELECT * FROM {{ref('dim_reporting_group_assignments')}}

),

products AS (

  SELECT * FROM {{ref('stg_app__products')}}

),

events__invited_psa AS (

  SELECT
    *,
    attributes:"product_id"::varchar AS product_id,
    attributes:"ended_at"::timestamp_ntz AS ended_at,
    attributes:"product_name"::varchar AS product_name
  FROM {{ref('dbt_events__invited_product_subscription_assignment')}}

),

events__onboarded_track AS (

  SELECT * FROM {{ref('dbt_events__onboarded_track')}}

),

events__activated_track AS (

  SELECT * FROM {{ref('dbt_events__activated_track')}}

),

join_product AS (

  SELECT DISTINCT
    i.member_id,
    MIN(i.event_at) AS invited_at,
    MAX(i.ended_at) AS ended_at,
    MAX(p.name) AS product_name
  FROM events__invited_psa AS i
  INNER JOIN products AS p
    ON i.product_id = p.product_id
  WHERE ((p.primary_coaching OR p.on_demand) OR (p.extended_network AND NOT p.care))
  GROUP BY i.member_id

)


SELECT
  {{ dbt_utils.surrogate_key(['rga.member_id', 'rga.reporting_group_id']) }} AS primary_key,
  rga.member_id,
  rga.reporting_group_id,
  MIN(p2.invited_at) as first_psa_starts_at,
  MIN(rga.starts_at) AS program_invite_at,
  IFF(BOOLOR_AGG(rga.member_is_open), MAX(p.ended_at), MAX(rga.ended_at)) AS lead_access_ended_at,
  IFF(BOOLOR_AGG(rga.ended_at IS NULL), NULL, MAX(rga.ended_at)) AS program_ended_at,
  MIN(a.event_at) AS activated_at,
  MIN(o.event_at) AS completed_onboarding_at,
  MAX(p.product_name) AS product_name
FROM reporting_group_assignments AS rga
LEFT JOIN join_product AS p
  ON rga.member_id = p.member_id AND
     rga.starts_at <= COALESCE(p.ended_at, DATEADD(DAY, 365, CURRENT_DATE())) AND
     (rga.ended_at >= p.invited_at OR rga.ended_at IS NULL)
LEFT JOIN join_product AS p2
  ON rga.member_id = p2.member_id
LEFT OUTER JOIN events__activated_track AS a
  ON rga.member_id = a.member_id AND
     a.event_at >= rga.starts_at AND
     (rga.ended_at IS NULL OR a.event_at < rga.ended_at)
LEFT OUTER JOIN events__onboarded_track AS o
  ON rga.member_id = o.member_id AND
     o.event_at >= rga.starts_at AND
     (rga.ended_at IS NULL OR o.event_at < rga.ended_at)
GROUP BY rga.member_id, rga.reporting_group_id