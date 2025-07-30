--with aggregated_daily_metrics as (
    select
        --primary key
        primary_key,

        --attributes
        date,
        date as report_date,
        campaign_name,
        ad_words_campaign_type,
        campaign_id,
        traffic_source,
        media_type,
        ad_account_name,
        data_source_type_name,
        sum(cost) as cost_usd,
        sum(clicks) as clicks,
        sum(impressions) as impressions
    from {{ ref('stg_funnel__ad_platform_campaigns_daily_metrics') }}
    {{ dbt_utils.group_by(n=10) }}