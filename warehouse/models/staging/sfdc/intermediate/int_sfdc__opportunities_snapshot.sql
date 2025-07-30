with opportunities_history as (

  select * from {{ ref('stg_sfdc__opportunities_snapshot') }}

),

dated_conversion_rates as (

  select * from {{ ref('stg_sfdc__dated_conversion_rates') }}

),

opportunity_stages as (

  select * from {{ ref('stg_sfdc__opportunity_stages') }}

),

sfdc_users_snapshot as (

  select * from {{ ref('int_sfdc__users_snapshots') }}

),

last_stage_changed_timestamp as (
  
  select
    sfdc_opportunity_id,
    max(created_at) as stage_last_changed_at
  from {{ ref('stg_sfdc__opportunity_field_history') }}
  where field = 'StageName'
  group by sfdc_opportunity_id
),

record_types as (
    select * from {{ ref('stg_sfdc__record_types') }}
),

opportunity_usd_currency_conversions as (

  select 
    o.history_primary_key,
    o.opportunity_type,
    o.opportunity_probability,
    o.opportunity_amount_unconverted / r.conversion_rate as opportunity_amount_usd,
    o.plan_renewal_amount_unconverted / r.conversion_rate as plan_renewal_amount_usd,
    o.first_year_acv_amount_unconverted / r.conversion_rate as first_year_acv_amount_usd,
    o.potential_value_unconverted / r.conversion_rate as potential_value_amount_usd,
    o.risk_amount_unconverted / r.conversion_rate as risk_amount_usd
  from opportunities_history o 
  left join dated_conversion_rates r
    on r.iso_code = o.currency_iso_code and r.conversion_rate_period_sequence = 1
),

currency_field_formulas as (

  /*
  renewal amount sfdc formula as of 2022-04-13 
  F(NOT(OR(ISPICKVAL(Type, "Renewal"),ISPICKVAL(Type, "Renewal Multi-Yr"),ISPICKVAL(Type, "Debook"))), 0,

  IF( AND(ISPICKVAL(Type, "Debook"), Plan_Renewal_Amount__c>=X1st_Year_ACV__c), Plan_Renewal_Amount__c,

  IF( AND(ISPICKVAL(Type, "Debook"), Plan_Renewal_Amount__c<X1st_Year_ACV__c), X1st_Year_ACV__c,

  IF( ISBLANK(Plan_Renewal_Amount__c), X1st_Year_ACV__c ,

  IF( X1st_Year_ACV__c >= Plan_Renewal_Amount__c, Plan_Renewal_Amount__c, X1st_Year_ACV__c ) ) )))


  expansion amount sfdc formula as of 2022-05-06
  IF(ISPICKVAL(Type,"Expansion"), X1st_Year_ACV__c , IF(AND(OR(ISPICKVAL(Type,"Renewal"),ISPICKVAL(Type,"Renewal Multi-Yr")), X1st_Year_ACV__c > Plan_Renewal_Amount__c ), X1st_Year_ACV__c -Plan_Renewal_Amount__c, IF( AND(ISPICKVAL(Type,"Debook"), Plan_Renewal_Amount__c <0),X1st_Year_ACV__c-Plan_Renewal_Amount__c ,0)) )
  */

  select
    o.history_primary_key,
    --expansion amount formula
    case
      when
        o.opportunity_type = 'Expansion'
          then first_year_acv_amount_usd
      when
        o.opportunity_type in ('Renewal','Renewal Multi-Yr')
        and coalesce(first_year_acv_amount_usd,0) > coalesce(plan_renewal_amount_usd,0)
          then first_year_acv_amount_usd - coalesce(plan_renewal_amount_usd,0)
      when
        opportunity_type in ('Debook')
        and coalesce(plan_renewal_amount_usd,0) < 0
          then first_year_acv_amount_usd - plan_renewal_amount_usd
      else 0 end as expansion_amount_usd,

    --renewal amount formula
    case
      when
        o.opportunity_type not in ('Renewal','Renewal Multi-Yr','Debook')
          then 0
      when
        o.opportunity_type = 'Debook'
        and coalesce(plan_renewal_amount_usd,0) >= coalesce(first_year_acv_amount_usd,0)
          then plan_renewal_amount_usd
      when
        o.opportunity_type = 'Debook'
        and coalesce(plan_renewal_amount_usd,0) < coalesce(first_year_acv_amount_usd,0)
          then first_year_acv_amount_usd
      --when
        --plan_renewal_amount_usd is null
          --then first_year_acv_amount_usd
      when
        coalesce(first_year_acv_amount_usd,0) >= coalesce(plan_renewal_amount_usd,0)
          then coalesce(plan_renewal_amount_usd,0)
      else first_year_acv_amount_usd end as renewal_amount_usd,
    --new business amount formula
    case
      when
        o.opportunity_type = 'New Business'
          then first_year_acv_amount_usd
      else 0 end as new_business_amount_usd,

    new_business_amount_usd + expansion_amount_usd as new_business_and_expansion_one_year_acv_usd,

    --Expected NewBus+Exp 1 ACV	formula
    new_business_and_expansion_one_year_acv_usd * opportunity_probability as expected_new_business_and_expansion_one_year_acv_usd

  from opportunity_usd_currency_conversions o

)

select 
  opportunities.* exclude(is_closed,is_won),
  iff(next_steps is not null, true,false) as has_next_steps,

  --flag to identify opportunities that meet our definition of pipeline
  iff(
    opportunities.opportunity_type in ('New Business', 'Renewal', 'Expansion')
    and currency_field_formulas.expected_new_business_and_expansion_one_year_acv_usd > 0
    and opportunities.reporting_stage not in ('0-Stage','1-Stage')
    and opportunities.reporting_stage is not null
    and opportunities.stage_2_date is not null, true, false
  ) as is_pipeline,

--flag to identify opportunities that meet our definition of coverage. Same as pipeline definition except we exclude closed lost opps
  iff(
    opportunities.opportunity_type in ('New Business', 'Renewal', 'Expansion')
    and currency_field_formulas.expected_new_business_and_expansion_one_year_acv_usd > 0
    and opportunities.reporting_stage not in ('0-Stage','1-Stage','Closed Lost')
    and opportunities.reporting_stage is not null
    and opportunities.stage_2_date is not null, true, false    
  ) is_coverage,
  curr.opportunity_amount_usd,
  curr.plan_renewal_amount_usd,
  curr.first_year_acv_amount_usd,
  curr.risk_amount_usd,
  curr.potential_value_amount_usd,

  currency_field_formulas.renewal_amount_usd,
  currency_field_formulas.new_business_amount_usd,
  currency_field_formulas.new_business_and_expansion_one_year_acv_usd,
  currency_field_formulas.expected_new_business_and_expansion_one_year_acv_usd,
  currency_field_formulas.expansion_amount_usd,

  executive_sponsor.name as executive_sponsor_name,
  executive_sponsor.user_role_name as executive_sponsor_role,

  opportunity_stages.is_closed,
  opportunity_stages.is_won,
  record_types.record_type_name,
  
  case
    when opportunity_stages.is_closed = false
      then true
  else false end as is_open,

  case 
    when opportunity_stages.is_closed = true and opportunity_stages.is_won = false
        then true
    else false end as is_lost,

  last_stage_changed_timestamp.stage_last_changed_at,
  --there are historical opportunities where close date on its own does not reflect the date that an
  --opportunity went closed lost. The below logic makes it so that close date for closed lost opps 
  --is always the last stage change date which will always be the closed lost date. 
  iff(is_lost and last_stage_changed_timestamp.stage_last_changed_at is not null,
      last_stage_changed_timestamp.stage_last_changed_at,close_date_raw) as close_date,

  opportunity_stages.forecast_category_name as stage_default_forecast_category

from 
opportunities_history opportunities
left join opportunity_usd_currency_conversions curr
  on curr.history_primary_key = opportunities.history_primary_key
left join currency_field_formulas
  on currency_field_formulas.history_primary_key = opportunities.history_primary_key
left join opportunity_stages 
  on opportunity_stages.api_name = opportunities.opportunity_stage_raw
left join record_types
    on opportunities.sfdc_record_type_id = record_types.sfdc_record_type_id
left join last_stage_changed_timestamp
  on last_stage_changed_timestamp.sfdc_opportunity_id = opportunities.sfdc_opportunity_id
left join sfdc_users_snapshot executive_sponsor
  on opportunities.executive_sponsor_id = executive_sponsor.sfdc_user_id
  and(
    (opportunities.valid_from > executive_sponsor.valid_to
    and opportunities.valid_to < executive_sponsor.valid_from)
    or opportunities.valid_to is null and executive_sponsor.valid_to is null)
where sfdc_account_id != '00150000025c8DOAAY' --this is a test account in sfdc
