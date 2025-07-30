{{
 config(
   tags=["eu"]
 )
}}


with coach_profiles as (

   select * from {{ source('coach', 'coach_profiles') }}

),

renamed AS (

   select
        -- ids
        id as coach_profile_id, --still functional but only for us data
        uuid as coach_profile_uuid,
        docebo_user_id,
        fountain_applicant_id,

        -- timestamps
        {{ load_timestamp('created_at') }},
        {{ load_timestamp('updated_at') }},
        {{ load_timestamp('member_endings_training_assigned_at') }},
        {{ load_timestamp('next_coach_nps_at') }},

        -- boolean coach types
        primary as is_primary_coach,
        consumer as is_consumer,
        coalesce(on_demand, false) as is_on_demand_coach,
        qa as is_qa_coach,
        care as is_care_coach,
        peer as is_peer_coach,
        "{{ environment_reserved_word_column('group') }}" as is_group_coach,

        -- qualifications / array fields
        additional_qualifications,
        account_qualifications,
        certification_qualifications,
        focus_qualifications,
        on_demand_qualifications,
        postgrad_qualifications,
        product_qualifications,
        professional_qualifications,
        segment_qualifications,
        group_coaching_qualifications,

        -- fields which will be replaced w Profile Island Attributes
        max_member_count,
        organization_block_list AS banned_organization_ids,
        appointment_buffer_enabled as has_appointment_buffer_enabled,
        engaged_member_count,
        current_volunteer_member_count,
        pick_rate,

        -- other
        bio as coach_bio,
        coach_style_words,
        endorsement,
        experience_highlight,
        greatest_accomplishment,
        videochat_storage_persistence_enabled,
        videochat_transcription_enabled,
        clinical_experience,
        invoices_with_shortlist,
        iff (industries = 'null',ARRAY_CONSTRUCT(),industries) as staffing_industries,
        languages as staffing_languages,
        iff (member_levels = 'null',ARRAY_CONSTRUCT(),member_levels) as staffing_member_levels,
        tier as staffing_tier,
        risk_level as staffing_risk_level,
        tier_backup,
        cohort,
        currency_code_backup,
        debrief360,
        external_shortlist_id,
        last_book_read,
        most_grateful_for,
        outlook,
        staffable_state,
        lgbtq,
        management_work_years,
        experience_country,
        clinical_corporate_years,
        industry_work_experience,
        coaching_hours,
        industry_coaching_experience,
        accredited_coaching,
        clinical_years,
        armed_forces,
        race_ethnicity,
        clinical_hours,
        additional_certifications,
        gender,
        function_coaching_experience,
        vp_leadership_org_size,
        topic_experience,
        exec_experience_org_size,
        function_work_experience,
        vp_leadership_years,
        accountability_style,
        new_member,
        resistant_member,
        professional_work_years,
        short_engagement,
        coaching_style,
        coaching_cloud,
        country_of_residence,
        segment_priority_level,
        hiring_tier,
        currency_code,
        group_coaching_session_url,
        coaching_varieties,
        consumer_priority_level,
        workshops_bio,
        eea_eligible as is_eea_eligible,
        fed_eligible as is_fed_eligible,
        preferred_weekly_hours,
        preferred_weekly_hours_updated_at


   from coach_profiles

)

select * from renamed
