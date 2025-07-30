{{
  config(
    tags=['classification.c3_confidential'],
    materialized='view'
  )
}}

WITH product_subscription_assignments AS (
    SELECT * FROM {{ref('int_app__product_subscription_assignments')}}
),

product_subscriptions AS (
    SELECT * FROM {{ref('stg_app__product_subscriptions')}}
),

products AS (
    SELECT * FROM {{ref('stg_app__products')}}
),

group_coaching_series AS (
    SELECT * FROM {{ref('stg_app__group_coaching_series')}}
),

track_assignments AS (
    SELECT * FROM {{ref('stg_app__track_assignments')}}
),

tracks AS (
    SELECT * FROM {{ref('dim_tracks')}}
),

group_coaching_series_track_assignments AS (
    SELECT * FROM {{ref('stg_app__group_coaching_series_track_assignments')}}
),

group_coaching_curriculums AS (
    SELECT * FROM {{ref('stg_app__group_coaching_curriculums')}}
),

group_coaching_cohorts AS (
    SELECT * FROM {{ref('stg_app__group_coaching_cohorts')}}
),

group_coaching_registrations AS (
    SELECT * FROM {{ref('stg_app__group_coaching_registrations')}}
),

join_registrations_and_cohorts AS (
    SELECT
        r.group_coaching_registration_id,
        c.group_coaching_cohort_id,
        c.group_coaching_series_id,
        r.user_id,
        r.created_at AS registration_created_at
    FROM group_coaching_registrations AS r
    INNER JOIN group_coaching_cohorts AS c ON r.group_coaching_cohort_id = c.group_coaching_cohort_id
    WHERE r.canceled_at IS NULL
),

join_group_coaching_series AS (
    SELECT s.group_coaching_series_id,
        s.registration_start,
        s.registration_ended_at,
        ta.track_assignment_id,
        ta.created_at ta_created_at,
        ta.ended_at ta_ended_at,
        ta.track_id,
        c.group_coaching_curriculum_id,
        rc.group_coaching_cohort_id,
        rc.group_coaching_registration_id,
        rc.registration_created_at,
        parse_json(c.TITLE_I18N):en::VARCHAR AS curriculum_title,
        ta.member_id,
        tracks.program_name
    FROM group_coaching_series AS s
    INNER JOIN group_coaching_series_track_assignments AS t ON s.group_coaching_series_id = t.group_coaching_series_id
    INNER JOIN track_assignments AS ta ON t.track_assignment_id = ta.track_assignment_id
    INNER JOIN group_coaching_curriculums AS c ON s.group_coaching_curriculum_id = c.group_coaching_curriculum_id
    INNER JOIN tracks ON ta.track_id = tracks.track_id
    LEFT JOIN join_registrations_and_cohorts AS rc ON s.group_coaching_series_id = rc.group_coaching_series_id AND ta.member_id = rc.user_id
),

join_product_subscription_assignments AS (
    SELECT
        psa.member_id,
        psa.product_subscription_assignment_id,
        psa.starts_at AS psa_starts_at,
        psa.ended_at AS psa_ended_at,
        ps.product_subscription_id,
        p.product_id,
        p.name product_name,
        p.workshops,
        p.coaching_circles
    FROM product_subscription_assignments AS psa
    INNER JOIN product_subscriptions AS ps ON psa.product_subscription_id = ps.product_subscription_id
    INNER JOIN products AS p ON ps.product_id = p.product_id
    WHERE coaching_circles
),

final AS (
    SELECT
        distinct
        {{ dbt_utils.surrogate_key(['js.member_id','js.group_coaching_series_id'
            ,'js.group_coaching_cohort_id','js.group_coaching_registration_id'
            ,'js.track_assignment_id','jpsa.product_subscription_assignment_id'])}}
                as group_coaching_assignment_id,
        js.group_coaching_series_id,
        js.registration_start,
        js.registration_ended_at,
        js.track_assignment_id,
        js.ta_created_at,
        js.ta_ended_at,
        js.track_id,
        js.group_coaching_curriculum_id,
        js.group_coaching_cohort_id,
        js.group_coaching_registration_id,
        js.registration_created_at,
        js.curriculum_title,
        js.member_id,
        js.program_name,
        jpsa.product_subscription_assignment_id,
        jpsa.psa_starts_at,
        jpsa.psa_ended_at,
        jpsa.product_subscription_id,
        jpsa.product_id,
        jpsa.product_name,
        jpsa.workshops,
        jpsa.coaching_circles
    FROM join_group_coaching_series AS js
    INNER JOIN join_product_subscription_assignments AS jpsa
      ON js.member_id = jpsa.member_id AND (
        (jpsa.psa_starts_at > js.registration_start AND (js.registration_ended_at IS NULL OR jpsa.psa_starts_at < js.registration_ended_at)) OR
        (jpsa.psa_starts_at < js.registration_start AND (jpsa.psa_ended_at IS NULL OR jpsa.psa_ended_at > js.registration_start)))
)

SELECT * FROM final
