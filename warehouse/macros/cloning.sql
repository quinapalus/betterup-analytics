{# Cloning Related Macros #}

{% macro clone_table_for_pr(source_database, source_schema, target_database, target_schema, table_name) %}
{# This macro is used to clone a table in Snowflake for a Pull Request environment. #}

    {# Define the SQL query to clone the table using the provided parameters. #}
    {% set clone_query %}
        CREATE OR REPLACE TRANSIENT TABLE {{ target_database }}.{{ target_schema }}.{{ table_name }} CLONE {{ source_database }}.{{ source_schema }}.{{ table_name }}
    {% endset %}
    
    {# Log the generated SQL query. #}
    {{ log("Cloning Begin: " ~ clone_query,info=true) }}
    {# Run the generated SQL query. #}
    {% do run_query(clone_query) %}
    {{ log("Cloning Complete",info=true) }}

{# End of the macro. #}
{% endmacro %}


{% macro recent_history_backup__clone_schemas(source_database, target_database) %}
    {% set schemas = [] %}
    {% set todays_date = [] %}
    {{ log('Source Database: ' ~ source_database, info=true) }}
    {{ log('Target Database: ' ~ target_database, info=true) }}
    
    {# /* Get Current Date for Schema Prefix, in YYYYMMDD Format */ #}
    {% set get_date_query %}
        SELECT TO_CHAR(CURRENT_DATE, 'YYYYMMDD') AS date
    {% endset %}

    {% set get_date_results = dbt_utils.get_query_results_as_dict(get_date_query) %}
    
    {%- if execute -%}
        {% set todays_date = get_date_results['DATE'][0] %}
    {%- endif -%}
    {{ log('Todays Date: ' ~ todays_date, info=true) }}

    {# /* Get a list of Schemas from the source database */ #}
    {# /* Set Schema List Query */ #}
    {% set get_schemas_query %}
        select schema_name
        from {{ source_database }}.information_schema.schemata
        where schema_name not like 'DBT%' -- DBT Schema, not needed
          and schema_name != 'INFORMATION_SCHEMA' -- System Schema, not needed
          and schema_name != 'ANALYTICS_APP' -- Manual Exclusion, not needed
    {% endset %}

    {# /* Execute query and store results as jinja dict */ #}
  	{% set get_schemas_results = dbt_utils.get_query_results_as_dict(get_schemas_query) %}

    {# /* Iterate through the results and append the schema names to the schemas list */ #}
    {% for k,v in get_schemas_results.items() %} {# /* k = column name, v = column value */ #}
        {% for i in v %} {# /* i = value in the column */ #}
            {{ schemas.append(i) }} {# /* Append the schema name to the schemas list */ #}
        {% endfor %}
    {% endfor %}

    {# /* Iterate through the schema names in the schemas list and clone each one into the target database,
        adding the suffix to the schema name */ #}
    {% for schema_name in schemas %}
        {% if schema_name != '' %}
        {# /* Create Clone Query */ #}
        {% set clone_query %}
            CREATE SCHEMA IF NOT EXISTS {{ target_database }}.{{ schema_name }}_{{ todays_date }} CLONE {{ source_database }}.{{ schema_name }};
        {% endset %}
        {{ log("Processing Clone: " ~ clone_query, info=true) }} {# /* removed brackets for log output demo */ #}
        {% do run_query(clone_query) %}
        {{ log("Clone Complete", info=true) }}
        {% endif %}
    {% endfor %}
{% endmacro %}

{% macro recent_history_backup__drop_old_clones(target_database) %}
    {% set schemas = [] %}
    {{ log('Target Database: ' ~ target_database, info=true) }}

    {# /* Get Cutoff Date [Current Date - 30 Days], in YYYYMMDD Format */ #}
    {% set get_date_query %}
        SELECT TO_CHAR(CURRENT_DATE - 30, 'YYYYMMDD') AS date
    {% endset %}
    {% set get_date_results = dbt_utils.get_query_results_as_dict(get_date_query) %}
    {%- if execute -%}
        {% set cutoff_date_result = get_date_results['DATE'][0] %}
    {%- endif -%}

    {%- set cutoff_date = cutoff_date_result | as_number -%}
    
    {# Log the target database and cutoff date #}
    {{ log("Target Database: " ~ target_database ~ ", Cutoff Date: " ~ cutoff_date, info=true) }}

    {# Get a list of Schemas from the target database that exceed the cutoff date #}
    {% set get_schemas_query %}
        SELECT schema_name
        FROM {{ target_database }}.information_schema.schemata
        WHERE schema_name NOT LIKE 'DBT%'
          AND schema_name != 'INFORMATION_SCHEMA'
          AND CAST(RIGHT(schema_name, 8) AS INTEGER) < {{ cutoff_date }}
    {% endset %}

    {% if execute %}
        {% set schemas = run_query(get_schemas_query).columns[0].values() %}
    {% endif %}

    {# /* Iterate through the schema names in schemas and drop them if they exceed the cutoff date */ #}
    {% for schema_name in schemas %}
        {% if schema_name != '' %}
            {{ log("Age Exceeds Cutoff Date: " ~ schema_name, info=true) }}
            {% set drop_query %}
                DROP SCHEMA IF EXISTS {{ target_database }}.{{ schema_name }};
            {% endset %}
            {{ log('Dropping Schema: ' ~ schema_name, info=true) }}
            {% do run_query(drop_query) %}
            {{ log('Schema Dropped: ' ~ schema_name, info=true) -}}
        {% endif %}
    {% endfor %}

{% endmacro %}