{{
  config(
    tags=['eu']
  )
}}

with product_subscription_assignments as (
  select * from {{ ref('dim_product_subscription_assignments') }}
),

contract_line_items as (
  select * from  {{ ref('dim_contract_line_items') }}
),

contract_line_item_details as (
  select 
    product_id,
    organization_id,
    min(starts_at) as min_starts_at,
    max(ends_at) as max_ends_at,
    sum(seats) as total_licenses_purchased,
    sum(seats * ifnull(coaching_months, 12) * 30.416) as total_purchased_days
  from contract_line_items
  where is_active
  group by product_id, organization_id
),

licenses_assigned as (
  select 
    cli.product_id,
    cli.organization_id,
    cli.total_licenses_purchased,
    cli.total_purchased_days,
    datediff('day', current_date(), cli.max_ends_at) as total_days_left_on_cli,
    count(psa.product_subscription_assignment_id) as total_licenses_assigned,
    count_if(psa.is_active) as total_active_psas_assigned,
    ifnull(sum(datediff('day', greatest(cli.min_starts_at, psa.starts_at), least(cli.max_ends_at, coalesce(psa.ended_at, psa.ends_at)))), 0) as total_license_days_assigned,
    ifnull(sum(datediff('day', greatest(cli.min_starts_at, psa.starts_at), least(current_date(), coalesce(psa.ended_at, psa.ends_at)))), 0) as actual_license_days_used
  from contract_line_item_details as cli
  left join product_subscription_assignments as psa
    on 
      cli.product_id = psa.product_id and 
      cli.organization_id = psa.organization_id and
      psa.starts_at <= cli.max_ends_at and 
      coalesce(psa.ended_at, psa.ends_at) >= cli.min_starts_at and
      psa.source = 'CPQ2'
  group by cli.product_id, cli.organization_id, cli.total_licenses_purchased, cli.total_purchased_days, total_days_left_on_cli
),

final as (
  select 
     {{ dbt_utils.surrogate_key(['product_id', 'organization_id']) }} as fact_active_contract_id,
    *,
    total_purchased_days - total_license_days_assigned as total_unused_license_days
  from licenses_assigned
)

select * from final