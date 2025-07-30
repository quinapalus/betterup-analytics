with metric_snapshots as (

    select * from {{ ref('snapshot_metrics') }}
    qualify row_number() over (partition by primary_key, date_trunc('day', dbt_valid_from)
                            order by dbt_valid_from desc) = 1
    --if there are multiple snapshots for a metric in a given day we want to only use the last one of the day for our daily snapshot

),

metric_snapshot_helper_fields as (

    select
        metric_snapshots.primary_key as metric_primary_key,
        metric_snapshots.metric_name,
        metric_snapshots.metric_date,
        metric_snapshots.metric_granularity,
        metric_snapshots.metric_value,

        metric_snapshots.dbt_valid_from as valid_from,
        metric_snapshots.dbt_valid_to as valid_to,

        --The version of the record captured in this row.
        --The initial version will be 1, the next snapshot of a metric that is inserted into the table will be version 2 and so on
        row_number() over(
            partition by metric_snapshots.primary_key
            order by metric_snapshots.dbt_valid_from) as version,

        --true if the row represents the most up to date snapshot of a metric
        metric_snapshots.dbt_valid_to is null as is_current_version

    from metric_snapshots
),

date_spine as (
    -- from https://github.com/dbt-labs/dbt-utils#date_spine-source

    {{ dbt_utils.date_spine(
    datepart ="day",
    start_date = "to_date('2023-03-20', 'YYYY-MM-DD')",
    end_date="current_date+1") }}
),

metric_daily_snapshots as (

    select
        {{ dbt_utils.surrogate_key(['metric_primary_key', 'date_day']) }}  as daily_snapshot_primary_key,
        date_trunc('day', date_day)                                   as as_of_date,

        metric_snapshot_helper_fields.*,

      --flag the rows that represent the snapshots for current date
        iff(as_of_date = max(date_trunc('day', date_day)) over(),true, false) as is_currently_valid,

      --the metric_value from the first time the metric was snapshotted  
      first_value(metric_value) over(
          partition by metric_snapshot_helper_fields.metric_primary_key
          order by date_trunc('day', date_day)) as first_snapshot_metric_value,

      --change in units since first time the metric was snapshotted
        metric_snapshot_helper_fields.metric_value - first_snapshot_metric_value as change_from_first_snapshot_in_units,

      --percent change since first time the metric was snapshotted
        (metric_snapshot_helper_fields.metric_value - first_snapshot_metric_value)
        / nullif(first_snapshot_metric_value,0) as percent_change_from_first_snapshot,

      --the metric_value for the prior day metric snapshot
        lag(metric_value,1) over(
                partition by metric_snapshot_helper_fields.metric_primary_key
                order by date_trunc('day', date_day)) as prior_day_snapshot_metric_value,

      --change in units since prior day metric snapshot
        metric_snapshot_helper_fields.metric_value - prior_day_snapshot_metric_value as change_from_prior_day_in_units,

      --the metric_value for the 7 days ago metric snapshot
        lag(metric_value,7) over(
                partition by metric_snapshot_helper_fields.metric_primary_key
                order by date_trunc('day', date_day)) as seven_days_prior_snapshot_metric_value,

      --change in units since seven days prior metric snapshot
        metric_snapshot_helper_fields.metric_value - seven_days_prior_snapshot_metric_value as change_from_seven_days_prior_in_units,

      --the metric_value for the 30 days prior metric snapshot
        lag(metric_value,30) over(
                partition by metric_snapshot_helper_fields.metric_primary_key
                order by date_trunc('day', date_day)) as thirty_days_prior_snapshot_metric_value,

      --change in units since 30 days days prior metric snapshot
        metric_snapshot_helper_fields.metric_value - thirty_days_prior_snapshot_metric_value as change_from_30_days_prior_in_units,

      --percent change since prior day metric snapshot
        (metric_snapshot_helper_fields.metric_value - prior_day_snapshot_metric_value)
        / nullif(prior_day_snapshot_metric_value,0) as percent_change_from_prior_day,

      --percent change since seven days prior metric snapshot
        (metric_snapshot_helper_fields.metric_value - seven_days_prior_snapshot_metric_value)
        / nullif(seven_days_prior_snapshot_metric_value,0) as percent_change_from_seven_days_prior,

      --percent change since thirty days prior metric snapshot
        (metric_snapshot_helper_fields.metric_value - thirty_days_prior_snapshot_metric_value)
        / nullif(thirty_days_prior_snapshot_metric_value,0) as percent_change_from_thirty_days_prior,

      --flag rows that have a regression
        iff(change_from_prior_day_in_units is not null and abs(change_from_prior_day_in_units) != 0, true,false) as is_regression_from_prior_day,
        iff(change_from_first_snapshot_in_units is not null and abs(change_from_first_snapshot_in_units) != 0, true,false) as is_regression_from_first_snapshot,
        iff(change_from_seven_days_prior_in_units is not null and abs(change_from_seven_days_prior_in_units) != 0, true,false) as is_regression_from_seven_days_prior_snapshot,
        iff(change_from_30_days_prior_in_units is not null and abs(change_from_30_days_prior_in_units) != 0, true,false) as is_regression_from_thirty_days_prior_snapshot

    from metric_snapshot_helper_fields
    inner join date_spine
      on metric_snapshot_helper_fields.valid_from::date <= date_spine.date_day
      and (metric_snapshot_helper_fields.valid_to::date > date_spine.date_day or metric_snapshot_helper_fields.valid_to is null)
      )

select * from metric_daily_snapshots
