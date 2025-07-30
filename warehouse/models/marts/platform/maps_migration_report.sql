
WITH product_subscription_assignments_migration_audits AS (

  SELECT * FROM {{ ref('int_app__product_subscription_assignment_migration_audits') }}

),

products AS (

  SELECT * FROM {{ ref('stg_app__products') }}

),

v1_unpivoted_by_features AS (

  SELECT * FROM
  (
  SELECT
      distinct ma.product_subscription_assignment_v1_id AS v1_id,
      primary_coaching, on_demand, extended_network, care,
      coaching_circles, workshops,
      product_subscription_assignment_v1_starts_at AS v1_starts_at ,
      product_subscription_assignment_v1_ends_at AS v1_ends_at ,
      product_subscription_assignment_v1_ended_at AS v1_ended_at
  FROM
  product_subscription_assignments_migration_audits AS ma
  LEFT JOIN products AS p_v1 ON ma.product_subscription_assignment_v1_product_id = p_v1.product_id
  )
  unpivot(v1_flags for coaching_type in (primary_coaching, on_demand, extended_network, care,coaching_circles, workshops))

),

v1_dates_by_features AS (

  SELECT v1_id, coaching_type, v1_flags,d.date
   FROM v1_unpivoted_by_features v1
   LEFT JOIN analytics.carina.dim_date d ON d.date BETWEEN v1.v1_starts_at AND coalesce(v1_ends_at, v1_ended_at)
  WHERE v1_flags

),

v2_unpivoted_by_features AS (

  SELECT * FROM
    (
    SELECT
        distinct ma.product_subscription_assignment_v1_id AS v1_id,
        primary_coaching, on_demand, extended_network, care,
        coaching_circles, workshops,
        product_subscription_assignment_v2_starts_at AS v2_starts_at ,
        product_subscription_assignment_v2_ends_at AS v2_ends_at ,
        product_subscription_assignment_v2_ended_at AS v2_ended_at
    FROM
    product_subscription_assignments_migration_audits AS ma
    LEFT JOIN products AS p_v2 ON ma.product_subscription_assignment_v2_product_id = p_v2.product_id
    )
  unpivot(v2_flags for coaching_type in (primary_coaching, on_demand, extended_network, care,coaching_circles, workshops))

),

v2_dates_by_features AS (

  SELECT v1_id, coaching_type, v2_flags,d.date
  FROM
  (
    SELECT v1_id, coaching_type, v2_flags,v2_starts_at,v2_ends_at,v2_ended_at
     FROM v2_unpivoted_by_features v2
    WHERE v2_flags
  ) v2 LEFT JOIN analytics.carina.dim_date d
  ON d.date BETWEEN v2.v2_starts_at AND coalesce(v2_ends_at, v2_ended_at)

),

v1_v2_mapped_by_feature_date AS (

 SELECT distinct v1.v1_id AS v1_id,v2.v1_id AS v1_id_associated_with_v2,
        v1.coaching_type v1_coaching_type,v2.coaching_type v2_coaching_type,
        v1.date v1_date,v2.date v2_date
   FROM v1_dates_by_features v1
   FULL OUTER JOIN v2_dates_by_features v2
   ON v1.v1_id = v2.v1_id AND v1.coaching_type = v2.coaching_type AND v1.date = v2.date
  ORDER BY v2_date, v1_date

)

SELECT DISTINCT COALESCE(v1_id,V1_ID_ASSOCIATED_WITH_V2 ) AS product_subscription_assignment_v1_id,
       IFF (v1_coaching_type IS NULL OR v2_coaching_type IS NULL,FALSE , TRUE ) AS has_equivalent_v2_psas
FROM v1_v2_mapped_by_feature_date

