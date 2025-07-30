with source as (

    select * from {% if env_var('DEPLOYMENT_ENVIRONMENT', '') == 'US Gov' %} 
                    {{ ref('base_fed_sfdc__programs') }} 
                  {% else %} 
                    {{ source('salesforce', 'programs') }}
                  {% endif %}

),

renamed as (

    select
        id as sfdc_program_id,
        name as sfdc_program_name,
        outcomes_bu_measured_c,
        program_resources_c,
        record_type_id,
        opportunity_c,
        participate_in_the_bu_community_c,
        securing_exposure_to_senior_leadership_c,
        dedicated_executive_licenses_c,
        dedicated_licenses_c,
        established_cadence_of_interaction_c,
        timely_execution_of_launch_deliverables_c,
        actively_managing_activation_engagement_c,
        population_focus_c,
        business_catalyst_c,
        main_contact_c,
        success_plan_link_c,
        they_can_express_what_is_working_and_why_c,
        co_creation_of_biz_case_or_roi_calc_c,
        impact_mindset_skill_behavior_changes_c,
        program_goals_c,
        sharing_info_with_other_stakeholders_c,
        clearly_articulates_problem_need_c,
        cohort_structure_and_timing_c,
        program_summary_c,
        special_success_plan_notes_c,
        asking_for_help_to_justify_new_use_cases_c,
        broadening_audience_for_value_story_c,
        can_articulate_their_big_why_c,
        explore_ask_good_q_s_about_behavior_data_c,
        connecting_members_growth_to_prog_goals_c,
        outcomes_customer_measured_c,
        exploring_new_opportunities_c,
        funding_source_c,
        is_deleted,
        last_modified_by_id,
        moving_beyond_activation_engagement_c,
        program_id_c,
        big_why_c,
        desired_outcome_c,
        impact_member_experience_c,
        links_to_tracks_c,
        account_c,
        created_by_id,
        other_licenses_c,
        engaged_with_tl_beyond_use_case_c,
        engaging_executive_sponsor_c,
        context_challenges_opportunities_needs_c,
        customer_journey_scene_c,
        group_licenses_c,
        key_features_in_use_c,
        solution_c,
        basic_licenses_c,
        standard_licenses_c,
        foundations_licenses_c,

        --dates
        contract_start_date_c::date as contract_start_date_c,
        contract_end_date_c::date as contract_end_date_c,
        next_qbr_date_c::date as next_qbr_date_c,
        next_rp_readiness_date_c::date as next_rp_readiness_date_c,
        last_activity_date::date as last_activity_date,

        --timestamps
        to_timestamp_ntz(created_date) as created_date,
        to_timestamp_ntz(last_referenced_date) as last_referenced_date,
        to_timestamp_ntz(last_modified_date) as last_modified_date,
        to_timestamp_ntz(system_modstamp) as system_modstamp,
        to_timestamp_ntz(uuid_ts) as uuid_ts
      
    from source

)

select * from renamed
