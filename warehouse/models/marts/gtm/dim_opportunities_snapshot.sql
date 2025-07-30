with opportunities_history as (

    select * from {{ ref('int_sfdc__opportunities_snapshot') }}

),

formula_fields as (

    select * from {{ ref('int_sfdc__opportunities_snapshot_formula_fields') }}
),

campaign_member_fields as (

    select * from {{ ref('int_opportunities_with_campaign_members')}}
),

closed_lost_dropout_stage as (

    select * from {{ ref('int_opportunity_closed_lost_dropout_stage')  }}
)

select 

  opportunities_history.*,

  --intervals from stage x to closed won or lost
  iff(opportunities_history.is_closed,datediff('day',opportunities_history.stage_2_date,opportunities_history.close_date),null) as days_from_stage_2_to_closed,
  iff(opportunities_history.is_closed,datediff('day',opportunities_history.stage_4_date,opportunities_history.close_date),null) as days_from_stage_4_to_closed,

  --intervals from stage x to closed won
  iff(opportunities_history.opportunity_stage = 'Closed Won',datediff('day',opportunities_history.stage_2_date,opportunities_history.close_date),null) as days_from_stage_2_to_closed_won,
  iff(opportunities_history.opportunity_stage = 'Closed Won',datediff('day',opportunities_history.stage_4_date,opportunities_history.close_date),null) as days_from_stage_4_to_closed_won,
  
  --intervals from stage x to closed lost
  iff(opportunities_history.opportunity_stage = 'Closed Lost',datediff('day',opportunities_history.stage_2_date,opportunities_history.close_date),null) as days_from_stage_2_to_closed_lost,
  iff(opportunities_history.opportunity_stage = 'Closed Lost',datediff('day',opportunities_history.stage_4_date,opportunities_history.close_date),null) as days_from_stage_4_to_closed_lost,
  
  --days between opportunity stages
  datediff('day',opportunities_history.stage_2_date,opportunities_history.stage_4_date) as days_from_stage_2_to_stage_4,
  datediff('day',opportunities_history.created_at,opportunities_history.stage_2_date) as days_from_stage_0_to_stage_2,
  datediff('day',opportunities_history.created_at,opportunities_history.stage_4_date) as days_from_stage_0_to_stage_4,


  --sfdc formula fields
  formula_fields.sales_region,
  formula_fields.new_business_deal_score,
  formula_fields.renewal_risk_score,
  formula_fields.primary_campaign_source_super_channel,
  formula_fields.qualification_score,

  --marketing attribution
  campaign_member_fields.super_channel,
  campaign_member_fields.management_targets_super_channel,
  campaign_member_fields.management_targets_line_of_business,
  campaign_member_fields.management_targets_geo,

  --other
  closed_lost_dropout_stage.closed_lost_dropout_stage

from opportunities_history

left join formula_fields 
    on formula_fields.history_primary_key = opportunities_history.history_primary_key
left join closed_lost_dropout_stage
    on closed_lost_dropout_stage.sfdc_opportunity_id = opportunities_history.sfdc_opportunity_id
left join campaign_member_fields
    on campaign_member_fields.sfdc_opportunity_id = opportunities_history.sfdc_opportunity_id
where opportunities_history.is_deleted = false
