with accounts as (
--to do: make this work for snapshotted data instead of just for current state.

  select * from {{ ref('int_sfdc__accounts_snapshot') }}
  where not is_deleted
  qualify row_number() over(partition by sfdc_account_id order by valid_from desc) = 1
)

select 
    child.sfdc_account_id,
    parent.account_type as parent_account_type,
    case
        when child.account_type = 'Customer' 
            then 'Customer'
        when parent.account_type = 'Customer' and child.account_type = 'Prospect'
            then 'Subsidiary'
        else null end as customer_account_type

from accounts as child
left join accounts as parent
    on child.parent_sfdc_account_id = parent.sfdc_account_id
