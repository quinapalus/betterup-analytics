       {{
  config(
    tags=['classification.c3_confidential'],
    materialized='table'
  )
}}

WITH member_cost_by_event AS (
    SELECT * FROM {{ref('member_cost_by_event')}}
),

contract_utilization AS (
    SELECT * FROM {{ref('contract_utilization')}}
),

product_subscription_assignments AS (
  SELECT * FROM  {{ref('int_app__product_subscription_assignments')}}
),

product_subscriptions AS (
  SELECT * FROM  {{ref('stg_app__product_subscriptions')}}
),

products AS (
  SELECT * FROM  {{ref('stg_app__products')}}
),

contracts AS (
  SELECT * FROM  {{ref('stg_app__contracts')}}
),

contract_line_items AS (
  SELECT * FROM  {{ref('stg_app__contract_line_items')}}
),

member_cost_to_contract AS (
SELECT
    coalesce(mc.organization_id,cu.organization_id) AS organization_id,
    coalesce(mc.member_id,cu.member_id) AS member_id,
    coalesce(mc.track_assignment_id,cu.track_assignment_id) AS track_assignment_id,
    coalesce(mc.track_id,cu.track_id) AS track_id,
    coalesce(mc.product_subscription_assignment_id,cu.product_subscription_assignment_id) AS product_subscription_assignment_id,
    --cost fields
    mc.billable_event_id,
    mc.event_type,
    mc.associated_record_type,
    mc.event_date,
    mc.event_reported_date,
    mc.sent_to_processor_date,
    mc.session_id,
    mc.session_date,
    mc.amount_due,
    mc.currency_code,
    mc.units,
    mc.usage_minutes,
    mc.coach_id,
    mc.payment_id,
    mc.response_body,
    -- group coaching fields
    mc.intervention_type,
    mc.group_coaching_series_id,
    mc.group_coaching_cohort_id,
    mc.session_order,
    mc.member_registered,
    mc.member_attempted_to_join,
    mc.max_registrants,
    mc.min_registrants,
    mc.registered_members_per_cohort,
    mc.attending_members_per_session,
    mc.unique_tracks_per_session,
    mc.unique_orgs_per_session,
    -- utilization fields
    cu.utilization_date,
    cu.utilized_seat_number,
    cu.is_over_utilized,
    cu.product_id,
    cu.contract_line_item_id,
    c.contract_id,
    cu.availability_date,
    cu.available_seat_number,
    -- product flags
    coalesce(p.primary_coaching, cu.primary_coaching) AS primary_coaching,
    coalesce(p.on_demand, cu.on_demand) AS on_demand,
    coalesce(p.extended_network, cu.extended_network) AS extended_network,
    coalesce(p.care, cu.care) AS care,
    coalesce(p.coaching_cloud, cu.coaching_cloud) AS coaching_cloud,
    coalesce(p.workshops, cu.workshops) AS workshops,
    coalesce(p.coaching_circles, cu.coaching_circles) AS coaching_circles
FROM contract_utilization AS cu
FULL OUTER JOIN member_cost_by_event AS mc
ON DATE(cu.utilization_date) = DATE(mc.event_reported_date)
AND cu.product_subscription_assignment_id = mc.product_subscription_assignment_id
LEFT OUTER JOIN product_subscription_assignments AS psa
ON mc.product_subscription_assignment_id = psa.product_subscription_assignment_id
LEFT OUTER JOIN product_subscriptions AS ps
ON ps.product_subscription_id = psa.product_subscription_id
LEFT OUTER JOIN products AS p
ON p.product_id = ps.product_id
LEFT OUTER JOIN contract_line_items AS cli
ON cli.contract_line_item_id = cu.contract_line_item_id
LEFT OUTER JOIN contracts AS c
ON c.contract_id = cli.contract_id
)
-- adding macro for product group logic
SELECT *, {{ sanitize_product_group('m.on_demand',
                                    'm.primary_coaching',
                                    'm.care',
                                    'm.coaching_circles',
                                    'm.workshops',
                                    'm.extended_network') }} AS product_group
FROM member_cost_to_contract AS m
