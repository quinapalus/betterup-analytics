{{
  config(
    tags=["eu"]
  )
}}

with organizations as (
    select * from {{ ref('stg_app__organizations') }}
),

tracks as (
    select * from {{ ref('stg_app__tracks') }}
),

concurrent_seat_ineligible_organizations as (
    select
        organization_id
    from tracks
    where
        deployment_type in ('private_pay','qa')
        or restricted
        or primary_coaching_months_limit is not null
    group by organization_id
)

select
    --ids
    organizations.organization_id,
    organizations.sfdc_account_id,
    organizations.reference_population_id,

    --categorical and text
    trim(organizations.name) as organization_name,
    organizations.risk_tier,
    organizations.account_segment,

    --dates and timestamps
    organizations.created_at,
    organizations.updated_at,
    organizations.product_subscriptions_enabled_at,
    organizations.v2_psa_enabled_at,

    --booleans
    concurrent_seat_ineligible_organizations.organization_id is null as is_eligible_for_concurrent_seats,
    organizations.product_subscriptions_enabled_at is not null as is_on_concurrent_seats,
    organizations.v2_psa_enabled_at is not null as has_migrated_to_maps
    
from organizations
left join concurrent_seat_ineligible_organizations
    on organizations.organization_id = concurrent_seat_ineligible_organizations.organization_id
