with source as (
    select * from {{ source('gsheets_betterup_metric_targets', 'metric_targets') }}
),

renamed as (
    select
        --primary
        {{ dbt_utils.surrogate_key(['target_month', 'metric_name', 'team', 'period_length'])}} as _unique,

        --foreign keys
        {{ dbt_utils.surrogate_key(['metric_name'])}} as metric_name_id,

        --attributes
        --target month is ingested and converted into a string using Stitch. This ensures it is a date.
        target_month::date as target_month,
        metric_name,
        team,
        target_metric_value__de as target_metric_value,
        units,
        period_length
    from source
)

select * from renamed