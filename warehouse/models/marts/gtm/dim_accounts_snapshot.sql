with accounts_history as (

    select * from {{ ref('int_sfdc__accounts_snapshot') }}

),

account_formulas as (

  select * from {{ ref('int_sfdc__accounts_snapshot_formula_fields') }}

),

parent_account_details as (

  select * from {{ ref('int_sfdc__parent_account_details') }}

)

select 

  accounts_history.*,

  --formula fields
  account_formulas.risk_rating,
  account_formulas.is_ultimate_parent_account,

  --parent account fields
  parent_account_details.parent_account_type,
  parent_account_details.customer_account_type

from accounts_history
left join account_formulas 
  on account_formulas.history_primary_key = accounts_history.history_primary_key
left join parent_account_details 
  on accounts_history.sfdc_account_id = parent_account_details.sfdc_account_id

where accounts_history.is_deleted = false
