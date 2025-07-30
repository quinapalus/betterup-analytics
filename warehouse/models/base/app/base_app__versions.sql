{{
    config(
        materialized='incremental',
        on_schema_change='fail',
    )
}}

-- The versions table is a log of app object states. This table is very large, so
-- loading incrementally while ignoring `unique_key` because versions is append-only

WITH src_versions AS (
    SELECT * FROM {{ source('app', 'second_versions') }}
    ),
{% if not is_incremental() %}
{# /* If a full refresh is ever required, it will leverage a backup of versions taken on 2023-03-27,
        after which the new 'second versions' table became active. */ #}
src_versions_historical AS (
    SELECT
        id AS version_id
        , created_at
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
        , whodunnit::int AS whodunnit
        , whodunnit_job
        , whodunnit_jid
    FROM {{ source('app_archive', 'versions') }}
    {%- if env_var('DEPLOYMENT_ENVIRONMENT', '') != 'US Gov' %}
    UNION
    SELECT
        id as version_id,
        created_at,
        event,
        item_id,
        item_uuid,
        item_type,
        object::variant as object,
        object_changes::variant as object_changes,
        user_agent,
        impersonated,
        impersonated_user_id,
        request_id,
        whodunnit::int as whodunnit,
        whodunnit_job,
        whodunnit_jid
    FROM {{ ref('base_app__second_versions_historical') }}
    {% endif %}
    ),
{% endif %}

versions AS (
    SELECT
         id AS version_id
        , created_at
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
        , whodunnit::int AS whodunnit
        , whodunnit_job
        , whodunnit_jid
    FROM src_versions

    {% if not is_incremental() %}
    UNION ALL

    SELECT
         id AS version_id
        , created_at
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
        , whodunnit::int AS whodunnit
        , whodunnit_job
        , whodunnit_jid
    FROM src_versions_historical
    {% endif %}
    )

SELECT *
FROM versions
{% if is_incremental() %}
  WHERE created_at > (SELECT max(created_at) FROM {{ this }})
{% endif %}
