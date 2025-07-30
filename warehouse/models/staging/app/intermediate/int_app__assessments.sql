{{
  config(
    tags=["eu"],
    materialized='table'
  )
}}

{% set response_attributes_strings = [
    "cancellation_reasons",
    "try_another_coach_option",
    "learn_how_to_pause",
    "session_overall_emotional",
    "why_dont_need_coach",
    "offering_a_discount",
    "package_would_like_to_see",
    "other_reasons_text_area",
    "why_unsatisfactory_experience",
    "other_different_packages_text_area",
    "other_reasons_dont_need_coach_text_area",
    "other_reasons_unsatisfactory_experience_text_area"
] -%}

with assessments as (

  select * from {{ref('stg_app__assessments')}}

),

gsheets_assessments as (

   select * from {{ ref('stg_gsheets_assessments__assessments') }}

),

destroyed_records as (
    select * from {{ref('stg_app__versions_delete')}}
    where item_type = 'Assessment'
),

final as (

    select
        assessment_id,
        user_id,
        creator_id,
        track_assignment_id,
        associated_record_id,
        associated_record_type,
        assessment_configuration_id,
        assessment_configuration_uuid,
        responses:appointment_id::int as assessment_session_id,
        type,
        responses,
        a_dims.assessment_name,
        a_dims.user_role,
        a_dims.assessment_description,
        parent_id,
        questions_version,
        shared_with_coach,
        submitted_at,
        expires_at,
        report_generated_at,
        created_at,
        updated_at,

        -- assessment sequences
        row_number() over (partition by assessments.user_id order by assessments.submitted_at) as assessment_sequence_by_user,
        row_number() over (partition by assessments.user_id order by assessments.submitted_at desc) as assessment_reverse_sequence_by_user,
        row_number() over (partition by assessments.user_id, assessments.type order by assessments.submitted_at) as assessment_sequence_by_user_and_type,
        row_number() over (partition by assessments.user_id, assessments.type order by assessments.submitted_at desc) as assessment_reverse_sequence_by_user_and_type,

        -- String response attributes - loop over response_attributes_strings list defined at top of model, 
        -- extract each from responses and convert to string
        {% for response_attribute in response_attributes_strings %}
        assessments.responses:{{ response_attribute }}::string as {{ response_attribute }},
        {% endfor %}

        -- Assessment Flags
        iff(assessments.type = 'Assessments::WholePersonProgramCheckinAssessment', true, false) as is_primary_reflection_point,
        iff(assessments.type in ('Assessments::WholePersonAssessment', 'Assessments::OnboardingAssessment', 'Assessments::PrimaryCoachingModalitySetupAssessment'), true, false) as is_onboarding,
        case when datediff(day, lag(submitted_at) over (partition by user_id, type order by submitted_at), submitted_at) < 30 then true else false end as is_duplicate_assessment,
        iff(
          assessments.type in ('Assessments::WholePersonAssessment',
                           'Assessments::WholePersonProgramCheckinAssessment',
                           'Assessments::WholePerson180Assessment',
                           'Assessments::WholePerson360Assessment',
                           'Assessments::WholePersonGroupCoachingCheckinAssessment'),
          true,false
        ) as is_whole_person_based_assessment,

        iff(assessments.type in ('Assessments::WholePerson180Assessment','Assessments::WholePerson360Assessment'),true,false) as is_multi_contributor_assessment,
        iff(assessments.type in ('Assessments::PostSessionMemberAssessment'), true, false) as is_post_session_assessment,
        iff(a_dims.assessment_name like '%Reflection Point',true,false) as is_any_reflection_point,
        iff(a_dims.assessment_name = 'Reflection Point',true,false) as is_primary_coaching_reflection_point,
        iff(a_dims.assessment_name in ('Whole Person Baseline', 'Onboarding', 'Primary Coaching Modality Setup Assessment', 'Onboarding Assessment 2.0'),true,false) as is_baseline_assessment,
        iff(a_dims.assessment_name = 'Group Coaching Reflection Point',true,false) as is_group_coaching_reflection_point,
        iff(a_dims.assessment_name = 'One-Month Survey',true,false) as is_one_month_survey,
        iff(is_whole_person_based_assessment, 'WPM ' || questions_version, null) as whole_person_model_version
        
    from assessments
    left outer join gsheets_assessments AS a_dims
        on assessments.type = a_dims.assessment_type
    left join destroyed_records
        on assessments.assessment_id = destroyed_records.item_id
    -- remove destroyed records
    where destroyed_records.item_id is null
    -- remove assessments that have not been submitted
    and assessments.submitted_at is not null

)

select * from final
