{{ config(
    tags=["eu"],
    schema="coach"
) }}

with metadata as (
    select * from {{ ref('progress_index__metadata') }}
),
metadata_eu as (
    select * from {{ source('analytics_eu_read_only_anon', 'anon_eu__progress_index__metadata') }}
),
metadata_unioned as (
    {% if env_var('DEPLOYMENT_ENVIRONMENT', '') == 'US Prod' %}
    -- this logic makes sure that data is combined only within the US Snowflake instance
        select * from metadata
        union
        select * from metadata_eu
    {% else %}
    -- this logic makes sure that data is not combined when in EU/Gov Snowflake instance as it coudl
    -- result in duplicate entries
        select * from metadata
    {% endif %}
)

select * from metadata_unioned
