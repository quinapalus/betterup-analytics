with daily_metrics as (

    select * from {{ source('funnel', 'ad_platform_campaigns_daily_metrics') }}

),

final as (
select 
--primary key
{{ dbt_utils.surrogate_key(['campaign', 'campaign_id', 'data_source_name','date', 'media_type']) }} as primary_key,

    date,
    campaign as campaign_name,
    campaign_type__adwords as ad_words_campaign_type,
    campaign_id,
    traffic_source,
    media_type,
    data_source_name as ad_account_name,
    data_source_type_name,
    cost,
    clicks,
    impressions,
    row_count  
from daily_metrics
--linked Ads on date 2022-06-22 and 22-08-16 contains duplicate records on this likely due to the records in question
--updating the number of row_counts for that day. It is an edge case and this qualify statement removes it. 
--It does not show up in the downstream fact table because it is aggregated and ignored. 
qualify(row_number() over (partition by campaign_name, campaign_id, ad_account_name, date, media_type order by row_count desc)) = 1
)

select * from final