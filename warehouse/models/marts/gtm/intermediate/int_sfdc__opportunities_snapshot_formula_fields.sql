{{
  config(
    tags=['run_test_true']
  )
}}

with opportunities_history as (

  select * from {{ ref('int_sfdc__opportunities_snapshot') }}

),

campaigns as (

  select * from {{ ref('stg_sfdc__campaigns') }}

),

categorical_text_field_formulas as (

  /*
  sales region formula as of 2022-04-15
    CASE ( Opp_Owner_Region__c,
  "ENT-Central", "NA Enterprise",
  "ENT-West", "NA Enterprise",
  "ENT-East", "NA Enterprise",
  "MidMarket", "NA Enterprise",
  "GAM", "Global Accounts",
  "SMB", "SMB",
  "ENT-Account Manager", "ENT-Existing Accounts",
  "ENT-Existing Accounts","ENT-Existing Accounts",
  "HQ / Admin", "HQ",
  "Channel", "Channel",
  "Care", "Care",
  "Undefined")

  */

select 
  o.history_primary_key,
  case 
    when
      opportunity_owner_region in ('ENT-Central','ENT-West','ENT-East','MidMarket')
      then 'NA Enterprise'
    when
      opportunity_owner_region = 'GAM' 
        then 'Global Accounts'
    when
      opportunity_owner_region = 'SMB' 
        then 'SMB'
    when
      opportunity_owner_region in ('ENT-Account Manager','ENT-Existing Accounts')
        then 'ENT-Existing Accounts'
    when
      opportunity_owner_region = 'HQ / Admin'
        then 'HQ'
    when
      opportunity_owner_region = 'Channel'
        then 'Channel'
    when 
      opportunity_owner_region = 'Care'
        then 'Care'
    else 'Undefined' end as sales_region

from opportunities_history o
),

score_formulas as (

/*
new business deal score formula as of 2022-04-15
( IF( Post_Covid_Discussion__c = TRUE, 20, 0)

+
IF( Catalyst_Identified__c = TRUE, 10, 0)

+
IF( Business_Case__c = TRUE, 10, 0)
+
IF( Budget_Confirmed__c = TRUE, 10, 0)
+
IF( C_Level__c = TRUE, 10, 0)
+
IF( Contract_in_Process__c = TRUE, 10, 0)
+
IF( Multi_Program__c = TRUE, 15, 0)
+
IF( Close_Plan_Agreed__c = TRUE, 15, 0))

renewal risk score formula as of 2022-04-15
(0.22 * IF( Catalyst_Identified__c = TRUE, 1, 10) )

+

(0.33 * IF( Multi_Program__c = TRUE, 1, 10) )

+

(0.17 * IF( ISPICKVAL( Highest_Champion_Level__c , "CHRO/LOB") , 1, IF(ISPICKVAL( Highest_Champion_Level__c , "VP L&D / OD"), 5, 10) ) )

+

(0.28 * IF( how_many_Champions__c > 3, 1, 10) )
  */

  select
    o.history_primary_key,

    --new business deal score
    case when has_post_covid_discussion then 20 else 0 end
    +
    case when has_catalyst_identified then 10 else 0 end 
    +
    case when has_business_case then 10 else 0 end 
    +
    case when has_budget_confirmed then 10 else 0 end  
    +
    case when has_c_level then 10 else 0 end
    +
    case when has_contract_in_process then 10 else 0 end
    +
    case when has_multi_program then 15 else 0 end 
    +
    case when has_close_plan_agreed then 15 else 0 end
    as new_business_deal_score,

    --renewal risk deal score

    0.22 * case when has_catalyst_identified then 1 else 10 end
    +
    0.33 * case when has_multi_program then 1 else 10 end
    +
    0.17 * case when highest_champion_level = 'CHRO/LOB' then 1 when highest_champion_level = 'VP L&D / OD' then 5 else 10 end
    +
    0.28 * case when how_many_champions > 3 then 1 else 10 end
    as renewal_risk_score,

     --qualification score - https://betterup.lightning.force.com/lightning/setup/ObjectManager/Opportunity/FieldsAndRelationships/00N2J00000AObTn/view
  {{ dbt_utils.safe_add('has_secured_customer_executive_sponsor','has_identified_customer_problem',
                        'has_proposed_solution','has_confirmed_commercial_proposal_scope',
                        'has_compelling_close_date','has_multiple_dmu_champions','has_bu_executive_sponsor',
                        'has_shared_business_case_roi','has_cfo_finance_approved_roi',
                        'has_confirmed_joint_engagement_plan','has_unique_competitive_differentiation',
                        'has_selected_betterup_as_vendor','has_confirmed_contracting_plan',
                        'has_confirmed_funding_budget','has_confirmed_deployment_date') }} as sum_of_qualification_items,

  sum_of_qualification_items / nullif(15.00,0) as qualification_score
  
  from opportunities_history o
)

select 

  o.history_primary_key,
  cat.sales_region,
  score.new_business_deal_score,
  score.renewal_risk_score,
  score.qualification_score,
  primary_campaign.super_channel as primary_campaign_source_super_channel

from opportunities_history o
left join categorical_text_field_formulas cat 
  on cat.history_primary_key = o.history_primary_key
left join score_formulas score 
  on score.history_primary_key = o.history_primary_key
left join campaigns as primary_campaign
  on primary_campaign.sfdc_campaign_id = o.primary_campaign_source_campaign_id
