WITH product_subscription_assignment_migration_audits AS (
  SELECT * FROM {{ ref('stg_app__product_subscription_assignment_migration_audits') }}
)

SELECT
  migration_audits_id,
  organization_id,
  user_id,
  contract_line_item_id,
  -- v1 fields
  product_subscription_assignment_v1_id,
  product_subscription_assignment_v1_product_id,
  product_subscription_assignment_v1_starts_at,
  product_subscription_assignment_v1_ends_at,
  product_subscription_assignment_v1_ended_at,
  -- v2 fields
  product_subscription_assignment_v2_id,
  product_subscription_assignment_v2_product_id,
  product_subscription_assignment_v2_starts_at,
  product_subscription_assignment_v2_ends_at,
  product_subscription_assignment_v2_ended_at,
  created_at,
  updated_at
FROM product_subscription_assignment_migration_audits AS psama
QUALIFY ROW_NUMBER() OVER
   (PARTITION BY product_subscription_assignment_v1_id, product_subscription_assignment_v2_product_id
        ORDER BY created_at DESC, product_subscription_assignment_v2_id DESC) = 1