--import CTEs
with marketing_spend as (
    select * from {{ ref('fact_ad_platform_campaigns_daily_metrics')}}
),

--KPI Calculation CTE
kpi_calculation as (
    select 
        date_trunc('month', report_date::date) as month,
        'digital_marketing_spend' as metric_name,
       {{ dbt_utils.surrogate_key(['metric_name'])}} as metric_name_id,

        --metric calculation
        sum(cost_usd) as metric_value
    from marketing_spend
    where ad_account_name in ('BetterUp D2C', 'BetterUp D2C - 3606714958')
        and data_source_type_name in ('AdWords', 'Facebook Ads', 'Bing')
        and traffic_source in ('Google', 'Bing', 'Facebook')
    group by 1, 2, 3
),

final as (
    select
        {{ dbt_utils.surrogate_key(['month', 'metric_name'])}} as _unique,
        kpi_calculation.*
    from kpi_calculation
)

select * from final