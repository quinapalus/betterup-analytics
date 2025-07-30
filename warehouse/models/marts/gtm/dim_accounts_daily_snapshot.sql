{{
  config(
    tags=['eu']
  )
}}

with date_spine as (
-- from https://github.com/dbt-labs/dbt-utils#date_spine-source

    {{ dbt_utils.date_spine(
    datepart ="day",
    start_date = "to_date('2022-04-14', 'YYYY-MM-DD')",
    end_date="current_date+1") }}
),

base as (

    select *
    from {{ ref('dim_accounts_snapshot') }}
    qualify row_number() over (partition by sfdc_account_id, DATE_TRUNC('day', valid_from)
                               order by valid_from desc) = 1
),

account_health_daily_snapshot as (

  select * from {{ ref('int_account_health_overall_score_daily_snapshot') }}

),

account_daily_snapshot as (

    select
      {{ dbt_utils.surrogate_key(['sfdc_account_id', 'date_day']) }}  as daily_snapshot_unique_key,
      date_trunc('day', date_day)                                   as as_of_date,
      iff(as_of_date = max(date_trunc('day', date_day)) over() and is_deleted = false, true, false) as is_currently_valid,
      base.*
    from base
    inner join date_spine
      on base.valid_from::date <= date_spine.date_day
      and (base.valid_to::date > date_spine.date_day or base.valid_to is null)
      )

select
  account_daily_snapshot.*,

  /*
  has_ever_been_eligible_for_account_health_scoring is checking if an account has ever been elegible for account health scoring
  This will be used to make sure that if an account becomes ineligible, the airflow job still updates the account 
  and sets the account health fields to null 
  */

  max(is_eligible_for_account_health_scoring) over(
            partition by account_daily_snapshot.sfdc_account_id) as has_ever_been_eligible_for_account_health_scoring,

  account_health_daily_snapshot.*
  --excluding duplicative/unnecasary columns
  exclude (sfdc_account_id, daily_snapshot_unique_key,valid_from,valid_to,
  is_current_version,is_currently_valid,version,is_last_snapshot_of_day,as_of_date,
  dbt_valid_to,dbt_valid_from,dbt_scd_id,dbt_updated_at)
  
from
account_daily_snapshot
left join account_health_daily_snapshot
  on account_daily_snapshot.daily_snapshot_unique_key = account_health_daily_snapshot.daily_snapshot_unique_key

