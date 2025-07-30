with mapping as (

   select * from {{ source('gsheets_coach_onboarding_inputs', 'mapping__decision_points') }}

)

select
    {{ dbt_utils.surrogate_key(['funnel_id','stage_id']) }} as primary_key,
    decision_point_stage_name,
    stage_id,
    funnel_name,
    funnel_id,
    regexp_replace(replace(lower(replace(decision_point_stage_name, ' - ', '_')), ' ', '_'), '[^a-z0-9_]', '') as decision_point_stage_name_cleaned


from mapping
