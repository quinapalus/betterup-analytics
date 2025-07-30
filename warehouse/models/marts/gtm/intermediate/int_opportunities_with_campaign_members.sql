with campaign_members as (
    select * 
    from {{ ref('int_campaign_members_with_campaigns') }} 
    where is_current_version and not is_deleted
    qualify row_number() over(partition by related_fcr_opportunity_id order by created_at desc) = 1
    /*
    there are are around 100 opportunities that have multiple campaign members. 
    Without this qualify statement the grain of this model would get thrown off.
    In the majority of these 100 or so cases, it would appear that the most recently 
    created campaign member is the correct one.
    */
),

opportunities as (
    select * from {{ ref('int_sfdc__opportunities_snapshot') }} 
    where is_current_version and not is_deleted
    qualify row_number() over(partition by sfdc_opportunity_id order by valid_from desc) = 1
    --this is an extra failsafe in case of duplicate issues in snapshot table. This is an issue that should be resolved in the dbt 1.0 update
),

opp_owner_region_lob_mapping as (

  select * from {{ ref('mapping__opportunity_owner_region_to_line_of_business') }}

)

select
    o.sfdc_opportunity_id,
    o.sfdc_account_id,
    o.created_at,
    o.stage_1_date,
    o.stage_2_date,
    o.stage_3_date,
    o.stage_4_date,
    o.stage_5_date,
    o.stage_6_date,
    o.close_date,
    o.opportunity_stage,
    o.opportunity_owner_region,
    cm.sfdc_person_id,
    cm.response_timestamp,
    cm.response_status,
    cm.campaign_member_status,
    cm.inquiry_date,
    cm.mql_date,
    cm.mql_timestamp,
    cm.sal_date,
    cm.sal_timestamp,
    cm.fm_date,
    cm.fm_timestamp,
    cm.inquiry_date_15_day_offset,
    cm.mql_date_15_day_offset,

    --campaign member details
    coalesce(cm.utm_campaign,'Unattributed') as utm_campaign,
    coalesce(cm.utm_content,'Unattributed') as utm_content,
    coalesce(cm.utm_medium,'Unattributed') as utm_medium,
    coalesce(cm.utm_source,'Unattributed') as utm_source,
    coalesce(cm.utm_term,'Unattributed') as utm_term,
    coalesce(cm.referrer_page,'Unattributed') as referrer_page,

    --campaign details
    coalesce(cm.publisher, 'Unattributed') as publisher,
    coalesce(cm.campaign_name, 'Unattributed') as campaign_name,
    coalesce(cm.channel, 'Unattributed') as channel,
    coalesce(cm.super_channel, 'Unattributed') as super_channel,
    coalesce(cm.marketing_program, 'Unattributed') as marketing_program,
    coalesce(cm.campaign_sourced_by, 'Unattributed') as campaign_sourced_by,
    coalesce(cm.tactic, 'Unattributed') as tactic,
    coalesce(cm.campaign_type, 'Unattributed') as campaign_type,
    coalesce(cm.audience, 'Unattributed') as audience,
    coalesce(cm.content_title, 'Unattributed') as content_title,
    coalesce(cm.mql_marketing_segment, 'Unattributed') as mql_marketing_segment,

    --parent campaign details 
    coalesce(cm.parent_campaign_publisher, 'Unattributed') as parent_campaign_publisher,
    coalesce(cm.parent_campaign_name, 'Unattributed') as parent_campaign_name,
    coalesce(cm.parent_campaign_channel, 'Unattributed') as parent_campaign_channel,
    coalesce(cm.parent_campaign_super_channel, 'Unattributed') as parent_campaign_super_channel,
    coalesce(cm.parent_campaign_marketing_program, 'Unattributed') as parent_campaign_marketing_program,
    coalesce(cm.parent_campaign_sourced_by, 'Unattributed') as parent_campaign_sourced_by,
    coalesce(cm.parent_campaign_tactic, 'Unattributed') as parent_campaign_tactic,
    coalesce(cm.parent_campaign_type, 'Unattributed') as parent_campaign_type,
    coalesce(cm.parent_campaign_audience, 'Unattributed') as parent_campaign_audience,
    coalesce(cm.parent_campaign_content_title, 'Unattributed') as parent_campaign_content_title,

    --management plan target composite key components
    case
      when o.opportunity_type in ('New Business','Expansion','Renewal Multi-Yr') and new_business_and_expansion_one_year_acv_usd > 0
           and new_business.opportunity_owner_region is not null
        then 'New/Upsell'
      when o.opportunity_type not in ('Pilot Expansion','Pilot','Debook','Donation','Trial') and new_business_and_expansion_one_year_acv_usd > 0
           and expansion.opportunity_owner_region is not null
        then 'Expand'
      else null end as management_targets_line_of_business,
    
    case
      when o.opportunity_owner_region = 'Government' 
        then 'Government'
      when o.opportunity_owner_region like '%EMEA%'
        then 'Europe'
      else 'North America' end as management_targets_geo,
      
    coalesce(case
      when management_targets_line_of_business = 'New/Upsell' 
           and coalesce(cm.super_channel, 'Unattributed') not in ('Unattributed','Referral/Alliances')
        then cm.super_channel
      when management_targets_line_of_business = 'New/Upsell' 
           and coalesce(cm.super_channel, 'Unattributed') in ('Unattributed','Referral/Alliances')
        then 'Sales'
      when management_targets_line_of_business = 'Expand'
           and coalesce(cm.super_channel, 'Unattributed') in ('Sales','Post Sales','Unattributed') 
        then 'Post Sales' else cm.super_channel end,'Unattributed') as management_targets_super_channel
          
from opportunities o 
left join campaign_members cm 
    on cm.related_fcr_opportunity_id = o.sfdc_opportunity_id
left join opp_owner_region_lob_mapping as new_business
  on new_business.opportunity_owner_region = o.opportunity_owner_region
     and new_business.line_of_business = 'New/Upsell'
left join opp_owner_region_lob_mapping as expansion
  on expansion.opportunity_owner_region = o.opportunity_owner_region
     and expansion.line_of_business = 'Expand'
