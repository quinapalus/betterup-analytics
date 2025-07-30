with issue_snapshots as (

    select * from {{ ref('int_jira__issues_snapshot') }}
    qualify row_number() over (partition by jira_issue_id, date_trunc('day', valid_from::timestamp_ntz)
                            order by valid_from desc) = 1
    --if there are multiple snapshots for an issue in a given day we want to only use the last one of the day for our daily snapshot

),

date_spine as (
    -- from https://github.com/dbt-labs/dbt-utils#date_spine-source

    {{ dbt_utils.date_spine(
    datepart ="day",
    start_date = "to_date('2023-04-13', 'YYYY-MM-DD')",
    end_date="current_date+1") }}
),

issue_daily_snapshots as (

    select

        {{ dbt_utils.surrogate_key(['jira_issue_id', 'date_day']) }}  as daily_snapshot_primary_key,
        date_trunc('day', date_day)                                   as as_of_date,
        issue_snapshots.*,
      --flag the rows that represent the snapshots for current date
        iff(as_of_date = max(date_trunc('day', date_day)) over(),true, false) as is_currently_valid

    from issue_snapshots
    inner join date_spine
      on issue_snapshots.valid_from::date <= date_spine.date_day
      and (issue_snapshots.valid_to::date > date_spine.date_day or issue_snapshots.valid_to is null)
)

select * from issue_daily_snapshots
