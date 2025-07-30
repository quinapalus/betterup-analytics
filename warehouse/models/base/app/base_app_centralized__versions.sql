WITH app_versions AS (
    SELECT * FROM {{ ref('base_app__versions') }}
    WHERE item_type IN (
                'CoachProfileSolution'
               , 'ProfileIslandAttribute'
               , 'BasePayRate'
               , 'CoachQualification'
               , 'CoachProfilePayRate')
),
version_versions AS (
    SELECT * FROM {{ ref('base_version__versions') }}
),
{%- if env_var('DEPLOYMENT_ENVIRONMENT', '') != 'US Gov' %}
versions_coach_profiles AS (
    SELECT * FROM {{ source('app_archive', 'versions_coach_profile') }}
),
versions_coach_profile_pay_rates AS (
    SELECT * FROM {{ source('app_archive', 'versions_coach_profile_pay_rates') }}
),
{%- endif -%}
centralized_versions_unioned AS (
    (SELECT
        version_id
        , 'app' AS source
        , created_at
        , event
        , item_id
        , item_uuid
        , CASE
            WHEN item_type = 'CoachProfileSolution' THEN 'Coach::CoachProfileSolution'
            WHEN item_type = 'ProfileIslandAttribute' THEN 'Coach::ProfileIslandAttribute'
            WHEN item_type = 'BasePayRate' THEN 'Coach::BasePayRate'
            WHEN item_type = 'CoachQualification' THEN 'Coach::CoachQualification'
            WHEN item_type = 'CoachProfile' THEN 'Coach::CoachProfile'
            WHEN item_type = 'CoachProfilePayRate' THEN 'Coach::CoachProfilePayRate'
            ELSE item_type END AS item_type
        , object
        , object_changes
        , user_agent
        , impersonated
        , impersonated_user_id
        , request_id
        , whodunnit
        , whodunnit_job
        , whodunnit_jid
    FROM app_versions)
 UNION
    (SELECT
        id AS version_id
        , 'app centralized' AS source
        , {{ load_timestamp('created_at') }}
        , event
        , item_id
        , item_uuid
        , item_type
        , try_parse_json(object) AS object
        , try_parse_json(object_changes) AS object_changes
        , user_agent
        , impersonated
        , impersonated_user_id
        , request_id
        , whodunnit
        , whodunnit_job
        , whodunnit_jid
    FROM version_versions)
{%- if env_var('DEPLOYMENT_ENVIRONMENT', '') != 'US Gov' %}
 UNION
    (SELECT
        version_id
        , 'app archive' AS source
        , created_at
        , event
        , item_id
        , item_uuid
        , item_type
        , object
        , object_changes
        , user_agent
        , impersonated
        , impersonated_user_id
        , request_id
        , whodunnit
        , whodunnit_job
        , whodunnit_jid
    FROM versions_coach_profiles)
 UNION
    (SELECT
        version_id
        , 'app archive' AS source
        , created_at
        , event
        , item_id
        , item_uuid
        , item_type
        , object
        , object_changes
        , user_agent
        , impersonated
        , impersonated_user_id
        , request_id
        , whodunnit
        , whodunnit_job
        , whodunnit_jid
    FROM versions_coach_profile_pay_rates)
{% endif -%}
)
SELECT
    {{ dbt_utils.surrogate_key(['version_id', 'source']) }} AS primary_key
    , *
FROM centralized_versions_unioned

