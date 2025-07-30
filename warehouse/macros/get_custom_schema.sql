-- override default schema with model schema in prod environment
-- https://docs.getdbt.com/docs/build/custom-schemas
-- https://docs.getdbt.com/reference/resource-configs/target_schema#faqs (snapshots)
-- If making changes to this file, please test them in the stg_analytics db since production object can be affected here

{% macro generate_schema_name_for_env(custom_schema_name, node) -%}

    {#
        Definitions:
            - target: target defined in profiles.yml
            - custom_schema_name: schema provided via dbt_project.yml or model config
            - target.schema: schema provided by the target defined in profiles.yml
    #}


    {%- if target.name in ('prod', 'stg', 'us-gov-staging') and custom_schema_name is not none -%}

        {{ custom_schema_name | trim }}

    {%- elif target.name in ('prod', 'stg', 'us-gov-staging') and custom_schema_name is none -%}

        {{ target.schema }}

    {%- elif target.name in ('dev','us-gov-dev') and custom_schema_name is none -%}

        {{ target.schema.lower() }}_{{ 'analytics'.lower() | trim }}

    {%- elif target.name in ('dev','us-gov-dev') and custom_schema_name is not none -%}

        {{ target.schema.lower() }}_{{ custom_schema_name | trim }}

    {%- else -%}

        {{ target.schema }}

    {%- endif -%}


{%- endmacro %}

{% macro generate_schema_name(schema_name, node=None) -%}
    {{ generate_schema_name_for_env(schema_name) }}
{%- endmacro %}