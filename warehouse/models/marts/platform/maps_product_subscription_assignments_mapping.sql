WITH product_subscription_assignments_migration_audits AS (

    SELECT * FROM {{ ref('int_app__product_subscription_assignment_migration_audits') }}

),

products AS (

    SELECT * FROM {{ ref('stg_app__products') }}

),

audits_products_joined AS (

    SELECT
        distinct ma.organization_id,
                 ma.product_subscription_assignment_v1_id,
                 ma.product_subscription_assignment_v2_id,
                 primary_coaching, on_demand, extended_network, care,
                 coaching_circles, workshops,
                 product_subscription_assignment_v2_starts_at AS v2_starts_at ,
                 product_subscription_assignment_v2_ends_at AS v2_ends_at ,
                 product_subscription_assignment_v2_ended_at AS v2_ended_at
    FROM product_subscription_assignments_migration_audits AS ma
    LEFT JOIN products AS p_v2
    ON ma.product_subscription_assignment_v2_product_id = p_v2.product_id

),

audits_products_with_coach_assignment_role AS (
    SELECT organization_id,
           product_subscription_assignment_v1_id,
           product_subscription_assignment_v2_id,
           v2_starts_at, v2_ends_at, v2_ended_at,
           IFF(v2_flag, {{ map_product_feature_to_coach_role('product_feature') }}, NULL) AS coach_assignment_role
    FROM audits_products_joined
    unpivot(v2_flag for product_feature in (primary_coaching, on_demand, extended_network, care,coaching_circles, workshops))
),
final AS (
    
    SELECT 
        {{ dbt_utils.surrogate_key(['organization_id','product_subscription_assignment_v1_id',
            'product_subscription_assignment_v2_id','coach_assignment_role'])}} as maps_product_subscription_assignments_mapping_id,
        organization_id,
        product_subscription_assignment_v1_id,
        product_subscription_assignment_v2_id,
        v2_starts_at, 
        v2_ends_at, 
        v2_ended_at,
        coach_assignment_role
    FROM audits_products_with_coach_assignment_role
    WHERE coach_assignment_role IS NOT NULL
    QUALIFY row_number() over (PARTITION BY product_subscription_assignment_v1_id,coach_assignment_role ORDER BY v2_starts_at) = 1
)
select * from final