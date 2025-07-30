with campaign_members as (
    select * from {{ ref('stg_sfdc__campaign_members_snapshot') }} where is_current_version and is_deleted = false
),

campaigns as (
    select * from {{ ref('int_campaigns_with_parent_campaigns')}} where is_deleted = false
),

persons as (
    select * from {{ ref('int_sfdc__leads_contacts_union') }} where is_current_version and is_deleted = false
)

select 

    campaign_members.*,
    --these offset dates are important GTM dimensions in looker. 
    case
        when day(inquiry_date) < 16
            then inquiry_date
        else dateadd(day,16,inquiry_date) end as inquiry_date_15_day_offset,

    case
        when day(mql_date) < 16
            then mql_date
        else dateadd(day,16,mql_date) end as mql_date_15_day_offset,

    persons.sfdc_account_id,
    persons.email,
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

    --parent campaign details
    campaigns.parent_campaign_publisher,
    campaigns.parent_campaign_name,
    campaigns.parent_campaign_channel,
    campaigns.parent_campaign_super_channel,
    campaigns.parent_campaign_marketing_program,
    campaigns.parent_campaign_sourced_by,
    campaigns.parent_campaign_tactic,
    campaigns.parent_campaign_type,
    campaigns.parent_campaign_audience,
    campaigns.parent_campaign_content_title

from campaign_members 
inner join persons 
    on persons.sfdc_person_id = campaign_members.sfdc_person_id
left join campaigns 
    on campaigns.sfdc_campaign_id = campaign_members.sfdc_campaign_id
