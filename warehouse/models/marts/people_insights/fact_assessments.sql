{{
  config(
    tags=["eu"]
  )
}}

with assessments as (

  select * from {{ ref('int_app__assessments') }}

),

assessment_configurations as (

    select * from {{ ref('stg_assessment__assessment_configurations') }}

),

track_assignments as (

  select * from {{ ref('dim_track_assignments') }}

),

members as (

  select * from {{ ref('dim_members') }}

),

final as (
    select
        a.assessment_id,
        a.user_id,
        a.creator_id,
        a.track_assignment_id,
        ta.track_id,
        a.associated_record_id,
        a.associated_record_type,
        a.assessment_configuration_id,
        a.assessment_configuration_uuid,
        a.whole_person_model_version,
        ac.title_en,
        a.type as assessment_type,
        a.responses,
        a.parent_id,
        a.questions_version,
        a.assessment_session_id,
        a.submitted_at,
        a.expires_at,
        a.report_generated_at,
        a.created_at,
        a.updated_at,
        a.shared_with_coach,
        a.assessment_name,
        a.user_role,
        a.assessment_description,

        -- response attributes that are strings
        a.cancellation_reasons,
        a.try_another_coach_option,
        a.learn_how_to_pause,
        a.session_overall_emotional,
        a.why_dont_need_coach,
        a.offering_a_discount,
        a.package_would_like_to_see,
        a.other_reasons_text_area,
        a.why_unsatisfactory_experience,
        a.other_different_packages_text_area,
        a.other_reasons_dont_need_coach_text_area,
        a.other_reasons_unsatisfactory_experience_text_area,
        
        -- lifetime program access fields for the member, used in track/RG filtering
        m.lifetime_track_ids,
        m.lifetime_track_names,
        m.lifetime_reporting_group_ids,

        -- Assessment Flags
        is_primary_reflection_point,
        is_primary_coaching_reflection_point,
        is_onboarding,
        is_baseline_assessment,
        is_duplicate_assessment,
        is_whole_person_based_assessment,
        is_multi_contributor_assessment,
        is_post_session_assessment,
        is_any_reflection_point,
        is_group_coaching_reflection_point,
        is_one_month_survey,

        -- assessment sequences
        a.assessment_sequence_by_user,
        a.assessment_reverse_sequence_by_user,
        a.assessment_sequence_by_user_and_type,
        a.assessment_reverse_sequence_by_user_and_type,
        dense_rank() over (partition by a.user_id, a.is_onboarding, a.is_primary_reflection_point order by a.submitted_at ) as assessment_sequence_by_onboarding_or_rp,
        dense_rank() over (partition by a.user_id, a.is_onboarding, a.is_primary_reflection_point order by a.submitted_at desc) as assessment_reverse_sequence_by_onboarding_or_rp,
        
        --date diffs
        datediff('day',m.confirmed_at, a.submitted_at) + 1 as days_between_member_activated_and_assessment_submitted,
        floor(days_between_member_activated_and_assessment_submitted / 30) as complete_months_between_member_activated_and_assessment_submitted,

        -- should be deprecated, including for testing / rollover
        ROW_NUMBER() OVER (PARTITION BY a.user_id, a.type, ta.track_id ORDER BY a.submitted_at DESC) AS track_assessment_reverse_sequence,
        iff(track_assessment_reverse_sequence = 1,true,false) as is_most_recent_assessment
    from assessments as a
    left outer join assessment_configurations ac
        on a.assessment_configuration_uuid = ac.assessment_configuration_uuid
    left outer join track_assignments ta
        on a.track_assignment_id = ta.track_assignment_id
    left outer join members m
        on m.member_id = a.user_id

)

select * from final
