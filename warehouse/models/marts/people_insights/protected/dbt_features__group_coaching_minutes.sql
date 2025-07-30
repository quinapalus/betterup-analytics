with group_coaching_sessions as(

    select * from {{ ref('stg_app__group_coaching_sessions') }}

),

group_coaching_appointments as(

    select * from {{ ref('stg_app__group_coaching_appointments') }}

),

group_coaching_cohorts as(

    select * from {{ ref('int_app__group_coaching_cohorts') }}

),

group_coaching_series as(

    select * from {{ ref('stg_app__group_coaching_series') }}

),

group_coaching_curriculums as(

    select * from {{ ref('stg_app__group_coaching_curriculums') }}

),

join_gc as (

    select
        s.group_coaching_session_id,
        s.starts_at,
        a.member_id,
        c.session_duration_minutes,
        cu.intervention_type
    from group_coaching_sessions as s
    inner join group_coaching_appointments as a on s.group_coaching_session_id = a.group_coaching_session_id
    inner join group_coaching_cohorts as c on s.group_coaching_cohort_id = c.group_coaching_cohort_id
    inner join group_coaching_series as sr on c.group_coaching_series_id = sr.group_coaching_series_id
    inner join group_coaching_curriculums as cu on sr.group_coaching_curriculum_id = cu.group_coaching_curriculum_id
    where a.attempted_to_join_at is not null

)

, final as (

    select
        {{ dbt_utils.surrogate_key(['member_id', 'group_coaching_session_id']) }} AS primary_key,
        member_id,
        group_coaching_session_id as associated_record_id,
        'groupcoachingsession' as associated_record_type,
        starts_at as feature_collected_at,
        concat('session_minutes_', intervention_type) as feature_key,
        object_construct('value', session_duration_minutes, 'units', 'minutes') as classification,
        object_construct('intervention_type', intervention_type) as feature_attributes,
        'time_on_platform' as feature_type
    from join_gc
    --Band-aid solution: fanout should be handled in CTEs or upstream
    group by 1,2,3,4,5,6,7,8

)

select * from final
