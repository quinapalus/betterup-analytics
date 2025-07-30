{{
  config(
    tags=["eu"]
  )
}}

WITH src_coach_profiles AS (
    SELECT * FROM {{ source('analytics_eu_read_only', 'anon_app_eu__coach_profiles') }}
),
coach_profiles AS (
    SELECT
         id AS coach_profile_id --still functional but only for US data
        , uuid AS coach_profile_uuid
        , {{ load_timestamp('created_at') }}
        , {{ load_timestamp('updated_at') }}
         , "{{ environment_reserved_word_column('group') }}"
        , account_qualifications
        , accountability_style
        , accredited_coaching
        , additional_certifications
        , additional_qualifications
        , appointment_buffer_enabled
        , armed_forces
        , bio
        , care
        , certification_qualifications
        , clinical_corporate_years
        , clinical_experience
        , clinical_hours
        , clinical_years
        , coach_style_words
        , coaching_cloud
        , coaching_hours
        , coaching_style
        , coaching_varieties
        , cohort
        , consumer
        , consumer_priority_level
        , country_of_residence
        , currency_code
        , currency_code_backup
        , current_volunteer_member_count
        , debrief360
        , docebo_user_id
        , eea_eligible
        , endorsement
        , engaged_member_count
        , exec_experience_org_size
        , experience_country
        , experience_highlight
        , external_shortlist_id
        , focus_qualifications
        , fountain_applicant_id
        , function_coaching_experience
        , function_work_experience
        , gender
        , greatest_accomplishment
        , group_coaching_session_url
        , group_coaching_qualifications
        , hiring_tier
        , industries AS staffing_industries
        , industry_coaching_experience
        , industry_work_experience
        , invoices_with_shortlist
        , languages AS staffing_languages
        , last_book_read
        , lgbtq
        , management_work_years
        , max_member_count
        , member_endings_training_assigned_at
        , member_levels AS staffing_member_levels
        , most_grateful_for
        , new_member
        , next_coach_nps_at
        , on_demand
        , on_demand_qualifications
        , organization_block_list AS banned_organization_ids
        , outlook
        , peer
        , pick_rate
        , postgrad_qualifications
        , primary AS type_primary
        , product_qualifications
        , professional_qualifications
        , professional_work_years
        , qa AS type_qa
        , race_ethnicity
        , resistant_member
        , risk_level AS staffing_risk_level
        , segment_priority_level
        , segment_qualifications
        , short_engagement
--        , staffable  -- doesn't exist in postgres anymore, all fields are NULL, we should remove from raw table
        , staffable_state
        , tier AS staffing_tier
        , tier_backup
        , topic_experience
        , videochat_storage_persistence_enabled
        , videochat_transcription_enabled
        , vp_leadership_org_size
        , vp_leadership_years
        , workshops_bio
    FROM src_coach_profiles
)

SELECT * FROM coach_profiles
