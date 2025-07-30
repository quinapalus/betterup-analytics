WITH opportunities AS (
  SELECT * FROM {{ ref('stg_sfdc__opportunities') }}
), 
users AS (
  SELECT * FROM {{ ref('stg_sfdc__users') }}
), 
user_roles AS (
  SELECT * FROM {{ ref('stg_sfdc__user_roles') }}
)

SELECT
  o.id AS sfdc_opportunity_id,
  o.account_id AS sfdc_account_id,
  o.name AS opportunity_name,
  o.amount::float as amount,
  {{ environment_varchar_to_timestamp('o.created_date','created_at') }},
  {{ environment_varchar_to_timestamp('o.last_modified_date','last_modified_at') }},
  o.fiscal_quarter,
  o.fiscal_year,
  o.type,
  o.type_c AS internal_type,
  o.stage_name,
  o.start_date_c::date AS start_date_estimate,
  o.renewal_date_c::date AS renewal_date_estimate,
  o.plan_renewal_amount_c AS plan_renewal_amount,
  o.is_won,
  o.is_closed,
  o.close_date::date as closed_at,
  o.close_date::date as close_date,
  o.start_date_per_agreement_c::date AS start_date_per_agreement,
  o.end_date_per_agreement_c::date AS end_date_per_agreement,
  o.win_reason_c AS win_reason,
  o.win_story_c AS win_reason_detail,
  o.lost_reason_c AS lost_reason,
  o.lost_reason_detail_c AS lost_reason_detail,
  o.is_deleted,
  o.opp_owner_region_c AS opp_owner_region,
  o.sales_region_c AS sales_region,
  o.forecast_category_c AS forecast_category,
  o.subscription_start_date_c::date AS subscription_start_date,
  o.subscription_end_date_c::date AS subscription_end_date,
  o.owner_id AS opportunity_owner_id,
  owners.name AS owner_name,
  owner_roles.role_name AS owner_role,
  -- Program Configuration
  o.default_resource_list_c AS default_resource_list,
  o.custom_whole_person_mapping_description_c AS custom_whole_person_mapping_description,
  -- Custom Program Design
  o.custom_program_design_c AS custom_program_design,
  o.program_design_description_c AS program_design_description,
  -- SMB
  o.member_hours_limit_of_hours_c AS member_hours_limit_of_hours,
  o.coach_tier_c AS staffing_tier,
  o.whole_person_360_enabled_c AS whole_person_360_enabled,
  o.video_chat_recording_enabled_c AS video_chat_recording_enabled,
  o.smb_deployment_cadence_c AS smb_deployment_cadence,
  o.smb_program_length_days_c AS smb_program_length_days,
  o.smb_enforce_hours_limit_c AS smb_enforce_hours_limit,
  o.smb_additional_info_partner_motivations_c AS smb_additional_info_partner_motivations,
  o.smb_engagement_type_c AS smb_engagement_type,
  o.smb_extended_network_c AS smb_extended_network,
  o.smb_manager_feedback_enabled_c AS smb_manager_feedback_enabled,
  o.smb_includes_behavioral_assessments_c AS smb_includes_behavioral_assessments,
  o.smb_additional_requirements_c AS smb_additional_requirements,
  o.smb_key_satisfaction_driver_c AS smb_key_satisfaction_driver
FROM opportunities AS o
LEFT JOIN users AS owners ON o.owner_id = owners.sfdc_user_id
LEFT JOIN user_roles AS owner_roles ON owners.sfdc_user_role_id = owner_roles.sfdc_user_role_id
WHERE NOT o.is_deleted
