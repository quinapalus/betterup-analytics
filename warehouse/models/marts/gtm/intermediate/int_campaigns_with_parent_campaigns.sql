with campaigns as (
    select * from {{ ref('stg_sfdc__campaigns') }} where is_deleted = false
)

select 
    campaigns.sfdc_campaign_id,
    campaigns.parent_sfdc_campaign_id,
    campaigns.publisher,
    campaigns.campaign_name,
    campaigns.channel,
    campaigns.super_channel,
    campaigns.marketing_program,
    campaigns.campaign_sourced_by,
    campaigns.tactic,
    campaigns.campaign_type,
    campaigns.audience,
    campaigns.content_title,
    campaigns.is_deleted,

    --immediate (one level up) parent campaign details.
    parent_campaigns.publisher as parent_campaign_publisher,
    parent_campaigns.campaign_name as parent_campaign_name,
    parent_campaigns.channel as parent_campaign_channel,
    parent_campaigns.super_channel as parent_campaign_super_channel,
    parent_campaigns.marketing_program as parent_campaign_marketing_program,
    parent_campaigns.campaign_sourced_by as parent_campaign_sourced_by,
    parent_campaigns.tactic as parent_campaign_tactic,
    parent_campaigns.campaign_type as parent_campaign_type,
    parent_campaigns.audience as parent_campaign_audience,
    parent_campaigns.content_title as parent_campaign_content_title

from campaigns
left join campaigns as parent_campaigns
    on campaigns.parent_sfdc_campaign_id = parent_campaigns.sfdc_campaign_id
