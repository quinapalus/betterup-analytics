with source as (
  select * from {{ source('app', 'lead_metadata_attributes') }}
),

renamed as (
select 
    --primary key
    id as lead_metadata_attribute_id,
    
    --foreign keys
    user_id,
    invitation_id,

    --attributes
    utm_params,
    msclkid,
    gclid,

    --parsed utm param values
    --previous logic leverages utm_source to attribute a marketing platform tag (Google, Facebook, Organic). 
    --A google sheet mapping will contain the translation between each unique utm_source into a proper channel attribution.
    lower(parse_json(utm_params)['utm_source']::string) as utm_source,
    lower(parse_json(utm_params)['utm_medium']::string) as utm_medium,
    lower(parse_json(utm_params)['utm_campaign']::string) as utm_campaign,
    lower(parse_json(utm_params)['utm_campaignid']::string) as utm_campaign_id,
    lower(parse_json(utm_params)['utm_adsetid']::string) as utm_adset_id,
    lower(parse_json(utm_params)['utm_adid']::string) as utm_ad_id,
    lower(parse_json(utm_params)['utm_term']::string) as utm_term,
    lower(parse_json(utm_params)['utm_content']::string) as utm_content,
    lower(parse_json(utm_params)['utm_platform']::string) as utm_platform,

    --channel attribution granularity
    --This will be a surrogate key created between the metadata and a sheet that will map the combination 
    --of these columns into a channel_attribution (facebook_ads, bing_ads, google_ads, linkedin_ads) at the user_id level.
    --Sheet exists at https://docs.google.com/spreadsheets/d/1XWTnZ7FWMtVbrQGfFNNgKjXVd-0IUsczgLlzLlw24BM/edit#gid=1750449271
    {{ dbt_utils.surrogate_key(['utm_source', 'utm_medium', 'utm_campaign', 'utm_content'])}} as app_channel_attribution_sk,

    --timestamps
    created_at,
    updated_at,

    --attribution order: This will allow downstream BI tools to select first or last touch attribution, or some combination in between.
    --Attribution order set to 1 represents first_touch_attribution. 
    row_number() over (partition by user_id order by created_at asc) as attribution_order,

    --attribution order: This will allow downstream BI tools to select first or last touch attribution, or some combination in between.
    --Attribution order set to 1 represents last_touch_attribution. 
    row_number() over (partition by user_id order by created_at desc) as reverse_attribution_order

from source
)

select * from renamed