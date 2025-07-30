{{ config(
    schema="coach"
) }}

WITH applicants AS (
    SELECT * FROM {{ ref('stg_fountain__applicants') }}
),
dei_coaches AS (
    SELECT * FROM {{ ref('dei_coaches') }}
),
coach_creation_data AS (
    SELECT
        a.fountain_applicant_id AS fountain_applicant_id,
        'https://www.fountain.com/betterup/applicants/' || a.fountain_applicant_id AS fountain_applicant_url,
        a.funnel AS funnel,
        a.first_name AS first_name,
        a.last_name AS last_name,
        a.time_zone AS time_zone,
        a.primary_email AS email,
        a.phone AS phone,
        'Coach' AS title,
        strtok_to_array('coach') AS roles,
        CASE
            WHEN time_zone = 'Beijing' THEN '578'
            ELSE '40'
        END AS organization_id,
        'associate' AS tier, -- set to associate tier
        'true' AS primary, -- default to primary set as true. Need to update for EN/On-demand
        'en' AS coaching_language, -- set desired language for this user as a member
        a.coaching_languages AS languages,
        a.member_level AS member_levels,
        a.coaching_industries AS industries,
        a.coaching_certifications AS coaching_certifications,
        a.non_icf_cert AS non_icf_cert,
        CASE WHEN a.mbti_certified = 'Yes' THEN 'MBTI'
        END AS certification_mbti,
        'not_reserved' AS risk_level,
        a.focus_area AS focus_areas,
        strtok_to_array('wpm2') AS products
    FROM applicants AS a
    WHERE a.stage = 'Create Coach Account'
    AND a.primary_email NOT IN (SELECT email from dei_coaches))
    
, coach_creation_data_with_links AS (
    SELECT
        fountain_applicant_id,
        funnel,
        first_name,
        last_name,
        fountain_applicant_url,
        CONCAT('https://app.betterup.co/admin/users/new?',
        {{ target.schema }}.encode_coach_creation_url(
            first_name,
            last_name,
            time_zone,
            email,
            phone,
            title,
            organization_id,
            tier,
            primary,
            coaching_language,
            roles,
            fountain_applicant_id,
            languages,
            member_levels,
            industries,
            risk_level,
            coaching_certifications,
            non_icf_cert,
            certification_mbti,
            focus_areas,
            products
        )) AS admin_url
    FROM coach_creation_data
)

SELECT * FROM coach_creation_data_with_links