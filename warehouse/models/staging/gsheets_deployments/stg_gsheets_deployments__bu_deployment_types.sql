WITH bu_deployment_types AS (

  SELECT * FROM {{ source('gsheets_deployments', 'bu_deployment_types') }}

),

renamed as (
SELECT
  deployment_type,
  deployment_name,
  {{ environment_null_if("accounting_category", "accounting_category") }},
  is_revenue_generating::boolean as is_revenue_generating,
  is_external::boolean as is_external,
  is_sales_trial::boolean as is_sales_trial
FROM bu_deployment_types
),

final as (
  select
  {{dbt_utils.surrogate_key(['deployment_type', 'deployment_name'])}} as _unique,
    *
  from renamed
)

select * from final
