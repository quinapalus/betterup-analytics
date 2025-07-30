{{
  config(
    tags=["eu"]
  )
}}

with coach_profiles as (

  select * from {{ ref('stg_coach__coach_profiles') }}

),

users as (

  select * from {{ ref('int_app__users') }}

),

profile_island_attributes as (

  select * from {{ ref('stg_coach__profile_island_attributes') }}

),

coach_profile_solutions as (

  select * from {{ ref('int_coach__coach_profile_solutions_rollup') }}

),

specialist_verticals as (

  select * from {{ ref('stg_curriculum__specialist_verticals') }}

),

{%- set qual_types = [
    'account', 'certification', 'focus', 'postgrad', 'product', 'professional',
    'segment'
    ]
-%}

qualification_arrays as ( -- this is for a legacy field: staffing_qualifications

  -- perform data type conversion on qualitifications fields:

  select
    coach_profile_uuid,
    coach_profile_id,
    additional_qualifications,

    {% for qual in qual_types -%}
      {{ qual}}_qualifications
      {%- if not loop.last -%},{% endif %}
    {% endfor %}
  from coach_profiles
),

-- unnest separated qualifications and re-compose legacy `staffing_qualifications` field
-- catchall "additional_qualifications" field is added back in in final query

quals as ( -- this is for a legacy field: staffing_qualifications
  {% for qual in qual_types -%}
    select
      coach_profile_uuid,
      coach_profile_id,
      '{{ qual }}_' || flattened.value as qual
    from qualification_arrays, lateral flatten (input => {{qual}}_qualifications) flattened

    {% if not loop.last %} union all {% endif %}
  {% endfor %}

),

staffing_qualifications as ( -- this is for a legacy field: staffing_qualifications

  select
    coach_profile_uuid,
    coach_profile_id,
    array_agg(qual) as staffing_qualifications
  from quals
  group by 1,2

),

-- The following 2 CTEs cleans the array field 'coaching_varieties' from coach_profiles,
 -- taking the field from a concatenated 'specialist_vertical_' + specialist_vertical_id (ex: 'specialist_vertical_9')
 -- to convert the descriptive key from specialist_verticals (ex: 'navigating_uncertainty')
 -- IF YOU MAKE ANY UPDATES TO THIS LOGIC, DO THE SAME IN INT_COACH__COACH_PROFILES_SNAPSHOT

coaching_varieties_unnested as (

  select
    cp.coach_profile_uuid,
    coaching_varieties.value::string as coaching_varieties_unnested
  from coach_profiles as cp,
    lateral flatten(input => cp.coaching_varieties) coaching_varieties

),

coaching_varieties_cleaned as (
  select
    cvu.coach_profile_uuid,
    array_agg(coalesce(sv.key, cvu.coaching_varieties_unnested)) as coaching_varieties
  from coaching_varieties_unnested cvu
  left join specialist_verticals sv
    on 'specialist_vertical_'||sv.specialist_vertical_id = cvu.coaching_varieties_unnested
  group by 1

),

final as (

    select
        -- coach profile ids
        cp.coach_profile_uuid,
        cp.coach_profile_id, -- still functional but only for US data (keeping for versions item_id join)
        cp.docebo_user_id,
        cp.fountain_applicant_id,

        -- user fields
        u.user_id as coach_id,
        u.user_uuid,
        u.first_name,
        u.last_name,
        u.first_name || ' ' || u.last_name as full_name,
        u.email,
        datediff(day, u.created_at, current_timestamp) as days_since_hire,
        u.deactivated_at,
        u.time_zone,
        u.tz_iana,
        u.country_code,
        u.country_name,
        u.subregion_m49,
        u.geo,

        -- coach profile timestamps
        cp.created_at,
        cp.updated_at,
        cp.member_endings_training_assigned_at,
        cp.next_coach_nps_at,

        -- separated staffing qualifications (coach profile array fields)
        qa.account_qualifications,
        qa.certification_qualifications,
        qa.focus_qualifications,
        qa.postgrad_qualifications,
        qa.product_qualifications,
        qa.professional_qualifications,
        qa.segment_qualifications,
        qa.additional_qualifications,
        cp.on_demand_qualifications,
        cp.group_coaching_qualifications,
        coaching_varieties_cleaned.coaching_varieties,

        -- maintain legacy staffing_qualifications field for downstream analytics
        array_cat(
          coalesce(sq.staffing_qualifications,array_construct()),
          coalesce(qa.additional_qualifications,array_construct())
        ) as staffing_qualifications,

        -- island attributes to coalesce
        coalesce(pia.max_member_count, cp.max_member_count) as max_member_count,
        coalesce(pia.banned_organization_ids, cp.banned_organization_ids) as banned_organization_ids,
        coalesce(pia.appointment_buffer_enabled, cp.has_appointment_buffer_enabled) as appointment_buffer_enabled,
        coalesce(pia.engaged_member_count, cp.engaged_member_count) as engaged_member_count,
        coalesce(pia.current_volunteer_member_count, cp.current_volunteer_member_count) as current_volunteer_member_count,
        coalesce(pia.pick_rate, cp.pick_rate) as pick_rate,

        --other island attributes
        pia.pending_primary_recommendation_count,

        -- boolean coach types
        is_primary_coach,
        is_consumer,
        is_on_demand_coach,
        is_qa_coach,
        is_care_coach,
        is_peer_coach,
        is_group_coach,

        -- coach profiles other
        coach_bio,
        coach_style_words,
        endorsement,
        experience_highlight,
        greatest_accomplishment,
        videochat_storage_persistence_enabled,
        videochat_transcription_enabled,
        clinical_experience,
        invoices_with_shortlist,
        staffing_industries,
        staffing_languages,
        staffing_member_levels,
        staffing_tier,
        staffing_risk_level,
        tier_backup,
        cohort,
        currency_code_backup,
        debrief360,
        external_shortlist_id,
        last_book_read,
        most_grateful_for,
        outlook,
        coalesce(pia.staffable_state, cp.staffable_state) as staffable_state,
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
        consumer_priority_level,
        workshops_bio,
        is_eea_eligible,
        is_fed_eligible,
        preferred_weekly_hours,
        preferred_weekly_hours_updated_at,

        --solutions
        cps.solution_keys_array,
        cps.has_solution_growth_and_transformation,
        cps.has_solution_sales_performance,
        cps.has_solution_diversity_equity_inclusion_and_belonging

    from coach_profiles as cp
    left outer join qualification_arrays as qa
        on cp.coach_profile_uuid = qa.coach_profile_uuid
    left outer join staffing_qualifications as sq
        on cp.coach_profile_uuid = sq.coach_profile_uuid
    inner join users as u
        on cp.coach_profile_uuid = u.coach_profile_uuid
    left join profile_island_attributes pia
        on pia.coach_profile_uuid = cp.coach_profile_uuid
    left join coach_profile_solutions cps
        on cp.coach_profile_uuid = cps.coach_profile_uuid
    left join coaching_varieties_cleaned
        on cp.coach_profile_uuid = coaching_varieties_cleaned.coach_profile_uuid
)

select * from final
