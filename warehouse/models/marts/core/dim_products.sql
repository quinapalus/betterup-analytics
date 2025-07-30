{{
  config(
    tags=['eu']
  )
}}

with products as (
    select * from {{ ref('stg_app__products') }}
)

select
    product_id,
    product_uuid,
    coaching_cloud,
    created_at,
    updated_at,
    description,
    extended_network,
    name,
    product_group,
    product_type,
    product_family,
    product_code,
    pricing_model,
    primary_solution,
    on_demand,
    primary_coaching,
    salesforce_product_identifier,
    workshops,
    source,
    specialist_catalog,
    specialist_coaching_limited,
    sessions_per_month,
    care,
    care_limited,
    coaching_circles,
    coaching_circles_limited,
    coaching_circles_limit
from products
