with accounts as (
    select * from {{ ref('snapshot_account_health_overall_score') }}
),

date_spine as (
-- from https://github.com/dbt-labs/dbt-utils#date_spine-source

    {{ dbt_utils.date_spine(
    datepart ="day",
    start_date = "to_date('2022-12-19', 'YYYY-MM-DD')",
    end_date="current_date+1") }}
),

accounts_with_snapshot_context as (

    select   
        accounts.*,

        dbt_valid_from as valid_from,
        dbt_valid_to as valid_to,
        dbt_valid_to is null as is_current_version,

        row_number() over(
            partition by sfdc_account_id
            order by dbt_valid_from
        ) as version,

        case when
            row_number() over(
            partition by sfdc_account_id,date_trunc('day',dbt_valid_from)
            order by dbt_valid_from desc) = 1 
            then true else false end as is_last_snapshot_of_day

    from accounts
)

select
    {{ dbt_utils.surrogate_key(['sfdc_account_id', 'date_day']) }}  as daily_snapshot_unique_key,
    date_trunc('day', date_day)                                  as as_of_date,
    iff(as_of_date = max(date_trunc('day', date_day)) over(), TRUE, FALSE)   as is_currently_valid,
    accounts_with_snapshot_context.*
from accounts_with_snapshot_context
inner join date_spine
    on accounts_with_snapshot_context.valid_from::date <= date_spine.date_day
    and (accounts_with_snapshot_context.valid_to::date > date_spine.date_day or accounts_with_snapshot_context.valid_to is null)
    and accounts_with_snapshot_context.is_last_snapshot_of_day
