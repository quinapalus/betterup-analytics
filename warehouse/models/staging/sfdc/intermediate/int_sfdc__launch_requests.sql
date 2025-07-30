with launch_requests as (
    select * from {{ ref('stg_sfdc__launch_requests') }}
    where not is_deleted
),

record_types as (
    select * from {{ ref('stg_sfdc__record_types') }}
),

accounts as (
    select * from {{ ref('stg_sfdc__accounts_snapshot') }}
    where is_current_version and not is_deleted
),

opportunities as (
    select * from {{ ref('stg_sfdc__opportunities_snapshot') }}
    where is_current_version and not is_deleted
)

select 
    launch_requests.*,
    accounts.sfdc_account_id,
    accounts.account_manager_id,
    accounts.account_csm_id,
    record_types.record_type_name

from launch_requests
left join record_types
    on launch_requests.sfdc_record_type_id = record_types.sfdc_record_type_id
left join opportunities
    on launch_requests.sfdc_opportunity_id = opportunities.sfdc_opportunity_id
left join accounts
    on opportunities.sfdc_account_id = accounts.sfdc_account_id
