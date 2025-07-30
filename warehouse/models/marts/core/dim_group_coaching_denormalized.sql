{{
  config(
    tags=["eu"]
  )
}}

WITH group_coaching_cohorts AS (
    SELECT * FROM {{ref('stg_app__group_coaching_cohorts')}}
),
group_coaching_registrations AS (
    SELECT * FROM {{ref('stg_app__group_coaching_registrations')}}
),
group_coaching_sessions AS (
    SELECT * FROM {{ref('stg_app__group_coaching_sessions')}}
),
group_coaching_series AS (
    SELECT * FROM {{ref('stg_app__group_coaching_series')}}
),
group_coaching_curriculums AS (
    SELECT * FROM {{ref('stg_app__group_coaching_curriculums')}}
), 
final AS (
    SELECT
        {{ dbt_utils.surrogate_key(['co.group_coaching_cohort_id','r.group_coaching_registration_id','s.group_coaching_session_id'])}} as group_coaching_denormalized_id,
        co.group_coaching_cohort_id,
        r.group_coaching_registration_id,
        r.created_at AS registered_at,
        r.canceled_at,
        s.group_coaching_session_id,
        s.starts_at,
        s.session_number,
        sr.group_coaching_series_id,
        sr.registration_start,
        sr.registration_end,
        cu.group_coaching_curriculum_id,
        parse_json(cu.TITLE_I18N):"en"::varchar as title,
        cu.intervention_type
    FROM group_coaching_cohorts AS co
    LEFT JOIN group_coaching_registrations AS r ON co.group_coaching_cohort_id = r.group_coaching_cohort_id
    LEFT JOIN group_coaching_sessions AS s ON co.group_coaching_cohort_id = s.group_coaching_cohort_id
    LEFT JOIN group_coaching_series AS sr ON co.group_coaching_series_id = sr.group_coaching_series_id
    LEFT JOIN group_coaching_curriculums AS cu ON sr.group_coaching_curriculum_id = cu.group_coaching_curriculum_id
)
SELECT * FROM final