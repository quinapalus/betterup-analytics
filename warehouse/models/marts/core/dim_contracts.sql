
WITH contracts AS (
  SELECT * FROM {{ref('stg_app__contracts')}}
),
contract_line_items AS (
  SELECT * FROM {{ref('dim_contract_line_items')}}
),
contract_line_item_dates as (
  SELECT
    contract_id,
    MIN(starts_at) as min_line_item_starts_at,
    MAX(coalesce(ends_at, dateadd(year, 1, greatest(starts_at, current_date)))) as max_line_item_ends_at
  FROM contract_line_items  
  GROUP BY contract_id
),
final as (
  SELECT
    {{ contract_key('c.contract_id') }} AS contract_key,
    c.contract_id AS app_contract_id,
    c.name AS contract_name,
    c.organization_id,
    c.created_at,
    c.updated_at,
    c.salesforce_contract_id,
    cli.min_line_item_starts_at,
    cli.max_line_item_ends_at
  FROM contracts AS c
  LEFT JOIN contract_line_item_dates cli 
    ON c.contract_id = cli.contract_id
)
select * from final