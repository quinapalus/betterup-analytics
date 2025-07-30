{{
  config(
    tags=["eu"]
  )
}}

with source as (
  select * from {{ source('app', 'tracks') }}
),
versions as (
  select * from {{ ref('stg_app__versions_delete') }}
),
deleted_tracks as (
  select
    v.item_id as track_id
    , parse_json(v.object):"created_at"
    , parse_json(v.object):"updated_at"
    , parse_json(v.object):"solution_uuid"
    , parse_json(v.object):"solution_id"
    , parse_json(v.object):"aggregate_hours_limit"
    , parse_json(v.object):"cached_account_tag_list"
    , parse_json(v.object):"cached_certification_tag_list"
    , parse_json(v.object):"cached_focus_tag_list"
    , parse_json(v.object):"cached_postgrad_tag_list"
    , parse_json(v.object):"cached_product_tag_list"
    , parse_json(v.object):"cached_professional_tag_list"
    , parse_json(v.object):"cached_segment_tag_list"
    , parse_json(v.object):"cached_tag_list"
    , parse_json(v.object):"coach_recommender_service_enabled"
    , parse_json(v.object):"contract_id"
    , parse_json(v.object):"customer_goals"
    , parse_json(v.object):"default_resource_list_id"
    , parse_json(v.object):"deployment_cadence"
    , parse_json(v.object):"deployment_coordinator_id"
    , parse_json(v.object):"deployment_owner_id"
    , parse_json(v.object):"deployment_type"
    , parse_json(v.object):"downloadable"
    , parse_json(v.object):"ends_on"
    , parse_json(v.object):"extended_network"
    , parse_json(v.object):"external_resources_link"
    , parse_json(v.object):"external_resource_video_links"
    , parse_json(v.object):"global_staffing_enabled"
    , parse_json(v.object):"includes_behavioral_assessments"
    , parse_json(v.object):"internal_notes"
    , parse_json(v.object):"key_satisfaction_driver"
    , parse_json(v.object):"languages"
    , parse_json(v.object):"launches_on"
    , parse_json(v.object):"length_days"
    , parse_json(v.object):"manager_feedback_enabled"
    , parse_json(v.object):"members_limit"
    , parse_json(v.object):"member_levels"
    , parse_json(v.object):"member_orientation"
    , parse_json(v.object):"minutes_limit"
    , parse_json(v.object):"minutes_used"
    , parse_json(v.object):"name"
    , parse_json(v.object):"num_reflection_points"
    , parse_json(v.object):"one_month_survey_enabled"
    , parse_json(v.object):"on_demand AS coaching_on_demand_enabled"
    , parse_json(v.object):"organization_id"
    , parse_json(v.object):"overview"
    , parse_json(v.object):"primary_coaching_enabled"
    , parse_json(v.object):"primary_coaching_months_limit"
    , parse_json(v.object):"program_name"
    , parse_json(v.object):"reflection_point_interval_appointments"
    , {{ environment_null_if('parse_json(v.object):"reflection_point_interval_days"', 'reflection_point_interval_days') }}
    , parse_json(v.object):"registration_template"
    , parse_json(v.object):"renewal_for_track_id"
    , parse_json(v.object):"request_seats"
    , parse_json(v.object):"restricted"
    , parse_json(v.object):"salesforce_opportunity_identifier"
    , parse_json(v.object):"salesforce_order_id"
    , parse_json(v.object):"session_count"
    , parse_json(v.object):"staffing_industries"
    , parse_json(v.object):"staffing_risk_levels"
    , parse_json(v.object):"staffing_tiers"
    , parse_json(v.object):"success_criteria"
    , parse_json(v.object):"supported_session_lengths"
    , parse_json(v.object):"use_cases"
    , parse_json(v.object):"videochat_recording_enabled"
    , parse_json(v.object):"videochat_storage_persistence_enabled"
    , parse_json(v.object):"videochat_transcription_enabled"
    , parse_json(v.object):"whole_person180_enabled"
    , parse_json(v.object):"whole_person360_enabled"
    , parse_json(v.object):"whole_person360_manager_included"
    , parse_json(v.object):"whole_person_model_2"
    , parse_json(v.object):"wpm_behavior_goals"
    , parse_json(v.object):"client_limit"
    , parse_json(v.object):"subscription_terms"
    , parse_json(v.object):"competency_mapping_id"
    , parse_json(v.object):"program_briefing_automatic_payment"
    , parse_json(v.object):"program_briefing_duration_minutes"
    , parse_json(v.object):"cached_on_demand_tag_list"
    , parse_json(v.object):"pulse_subdimensions"
    , parse_json(v.object):"partner_emails"
    , parse_json(v.object):"account_executive_id"
    , parse_json(v.object):"account_manager_id"
    , parse_json(v.object):"solution_consultant_id"
    , parse_json(v.object):"coaching_cloud"
    , parse_json(v.object):"estimated_seat_count"
    , parse_json(v.object):"stripe_pricing_plan_id"
    , parse_json(v.object):"valid_for_coaching"
    , parse_json(v.object):"invite_track_id"
    , parse_json(v.object):"track_assignments_activate_on_invite"
    , parse_json(v.object):"launch_status"
    , parse_json(v.object):"default_product_subscription_id"
    , parse_json(v.object):"default_product_subscription_ends_at"
    , parse_json(v.object):"manager_required_360"
    , parse_json(v.object):"care"
    , parse_json(v.object):"peer"
    , parse_json(v.object):"eap_instructions_markdown_i18n"
    , parse_json(v.object):"salesforce_program_identifier"
    , parse_json(v.object):"crisis_support_markdown_i18n"
    , parse_json(v.object):"required_primary_coach_learning_ext_course_id"
    , parse_json(v.object):"complexity"
    , parse_json(v.object):"track_template"
    , parse_json(v.object):"disable_confirmation_email"
    , parse_json(v.object):"custom_onboarding_section4_id"
    , parse_json(v.object):"custom_onboarding_section_id"
    , parse_json(v.object):"custom_onboarding_section1_id"
    , parse_json(v.object):"custom_onboarding_section3_id"
    , parse_json(v.object):"custom_onboarding_manager_feedback_section_id"
    , parse_json(v.object):"custom_onboarding_team_insights_section_id"
    , parse_json(v.object):"custom_onboarding_section2_id"
    , parse_json(v.object):"custom_manager_feedback_request_id"
    , parse_json(v.object):"allow_recurring_weekly45min_sessions"
    , parse_json(v.object):"features"
    , parse_json(v.object):"use_experience_configs"
    , parse_json(v.object):"community"
    , parse_json(v.object):"max_invites"
    , parse_json(v.object):"dependent_invite_track_id"
    , parse_json(v.object):"parent_track_id"
    , true as is_deleted
  from versions v
  where v.item_type = 'Track'
),
tracks as (
  select
      id
    , created_at
    , updated_at
    , solution_uuid
    , solution_id
    , aggregate_hours_limit
    , cached_account_tag_list
    , cached_certification_tag_list
    , cached_focus_tag_list
    , cached_postgrad_tag_list
    , cached_product_tag_list
    , cached_professional_tag_list
    , cached_segment_tag_list
    , cached_tag_list
    , coach_recommender_service_enabled
    , contract_id
    , customer_goals
    , default_resource_list_id
    , deployment_cadence
    , deployment_coordinator_id
    , deployment_owner_id
    , deployment_type
    , downloadable
    , ends_on
    , extended_network
    , external_resources_link
    , external_resource_video_links
    , global_staffing_enabled
    , includes_behavioral_assessments
    , internal_notes
    , key_satisfaction_driver
    , languages
    , launches_on
    , length_days
    , manager_feedback_enabled
    , members_limit
    , member_levels
    , member_orientation
    , minutes_limit
    , minutes_used
    , name
    , num_reflection_points
    , one_month_survey_enabled
    , on_demand
    , organization_id
    , overview
    , primary_coaching_enabled
    , primary_coaching_months_limit
    , program_name
    , reflection_point_interval_appointments
    , {{ environment_null_if("reflection_point_interval_days", "reflection_point_interval_days") }}
    , registration_template
    , renewal_for_track_id
    , request_seats
    , restricted
    , salesforce_opportunity_identifier
    , salesforce_order_id
    , session_count
    , staffing_industries
    , staffing_risk_levels
    , staffing_tiers
    , success_criteria
    , supported_session_lengths
    , use_cases
    , videochat_recording_enabled
    , videochat_storage_persistence_enabled
    , videochat_transcription_enabled
    , whole_person180_enabled
    , whole_person360_enabled
    , whole_person360_manager_included
    , whole_person_model_2
    , wpm_behavior_goals
    , client_limit
    , subscription_terms
    , competency_mapping_id
    , program_briefing_automatic_payment
    , program_briefing_duration_minutes
    , cached_on_demand_tag_list
    , pulse_subdimensions
    , partner_emails
    , account_executive_id
    , account_manager_id
    , solution_consultant_id
    , coaching_cloud
    , estimated_seat_count
    , stripe_pricing_plan_id
    , valid_for_coaching
    , invite_track_id
    , track_assignments_activate_on_invite
    , launch_status
    , default_product_subscription_id
    , default_product_subscription_ends_at
    , manager_required_360
    , care
    , peer
    , eap_instructions_markdown_i18n
    , salesforce_program_identifier
    , crisis_support_markdown_i18n
    , required_primary_coach_learning_ext_course_id
    , complexity
    , track_template
    , disable_confirmation_email
    , custom_onboarding_section4_id
    , custom_onboarding_section_id
    , custom_onboarding_section1_id
    , custom_onboarding_section3_id
    , custom_onboarding_manager_feedback_section_id
    , custom_onboarding_team_insights_section_id
    , custom_onboarding_section2_id
    , custom_manager_feedback_request_id
    , allow_recurring_weekly45min_sessions
    , features
    , use_experience_configs
    , community
    , max_invites
    , dependent_invite_track_id
    , parent_track_id
    , false as is_deleted
  from source
),
unioned_tracks as (
  select * from tracks
  union
  select * from deleted_tracks
),
renamed as (
    select
        id as track_id,
        {{ load_timestamp('created_at') }},
        {{ load_timestamp('updated_at') }},
        solution_uuid,
--        solution_id,   -- this ID is still functional but only for US data. We decided to only expose and use the
                         -- solution_uuid since that one can be used in the US and EU instance
        aggregate_hours_limit,
        cached_account_tag_list,
        cached_certification_tag_list,
        cached_focus_tag_list,
        cached_postgrad_tag_list,
        cached_product_tag_list,
        cached_professional_tag_list,
        cached_segment_tag_list,
        cached_tag_list,
        coach_recommender_service_enabled,
        contract_id,
        customer_goals,
        default_resource_list_id,
        deployment_cadence,
        deployment_coordinator_id,
        deployment_owner_id,
        deployment_type,
        downloadable,
        {{ load_timestamp('ends_on') }},
        extended_network AS coaching_extended_network_enabled,
        external_resources_link,
        external_resource_video_links,
        global_staffing_enabled,
        includes_behavioral_assessments,
        internal_notes,
        key_satisfaction_driver,
        languages,
        launches_on,
        length_days,
        manager_feedback_enabled,
        members_limit,
        member_levels,
        nullif(member_orientation, ''),
        minutes_limit,
        minutes_used,
        trim(name) as name,
        num_reflection_points,
        one_month_survey_enabled,
        on_demand AS coaching_on_demand_enabled,
        organization_id,
        overview,
        primary_coaching_enabled AS coaching_primary_enabled,
        primary_coaching_months_limit,
        trim(program_name) as program_name,
        reflection_point_interval_appointments,
        reflection_point_interval_days,
        registration_template,
        renewal_for_track_id,
        request_seats,
        restricted,
        NULLIF(salesforce_opportunity_identifier, '') AS sfdc_opportunity_id,
        salesforce_order_id,
        session_count,
        staffing_industries,
        staffing_risk_levels,
        staffing_tiers,
        success_criteria,
        supported_session_lengths,
        use_cases,
        videochat_recording_enabled as is_videochat_recording_enabled,
        videochat_storage_persistence_enabled,
        videochat_transcription_enabled,
        whole_person180_enabled,
        whole_person360_enabled,
        whole_person360_manager_included,
        whole_person_model_2,
        wpm_behavior_goals,
        client_limit,
        subscription_terms,
        competency_mapping_id,
        program_briefing_automatic_payment,
        program_briefing_duration_minutes,
        cached_on_demand_tag_list,
        pulse_subdimensions,
        partner_emails,
        account_executive_id,
        account_manager_id,
        solution_consultant_id,
        coaching_cloud,
        estimated_seat_count,
        stripe_pricing_plan_id,
        valid_for_coaching,
        invite_track_id,
        track_assignments_activate_on_invite,
        launch_status,
        default_product_subscription_id,
        {{ load_timestamp('default_product_subscription_ends_at') }},
        manager_required_360,
        care,
        peer,
        eap_instructions_markdown_i18n,
        salesforce_program_identifier,
        crisis_support_markdown_i18n,
        required_primary_coach_learning_ext_course_id,
        complexity,
        track_template,
        disable_confirmation_email,
        custom_onboarding_section4_id,
        custom_onboarding_section_id,
        custom_onboarding_section1_id,
        custom_onboarding_section3_id,
        custom_onboarding_manager_feedback_section_id,
        custom_onboarding_team_insights_section_id,
        custom_onboarding_section2_id,
        custom_manager_feedback_request_id,
        allow_recurring_weekly45min_sessions,
        features,
        use_experience_configs,
        community,
        max_invites,
        dependent_invite_track_id,
        parent_track_id,
        is_deleted
    from unioned_tracks
)

select * from renamed