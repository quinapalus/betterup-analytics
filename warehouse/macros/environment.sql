{%- macro environment_reserved_word_column(column) -%}
    {%- set col_name = column -%}
    {%- if env_var('DEPLOYMENT_ENVIRONMENT', '') == 'US Gov' -%}
        {%- set col_name = column.lower() -%}
    {%- else -%}
        {%- set col_name = column.upper() -%}
    {%- endif -%}
    {{- col_name -}}
{%- endmacro -%}

{#- /* This macro replaces a column reference with NULL,
        this is due to some columns existing historically on commercial
        snowflake, but not on EU or Gov instances.
        See docs: https://betterup.atlassian.net/wiki/spaces/DATA/pages/3350528149/One-Project+Many-Targets+Environment+Differences */ #}
{%- macro environment_null_if(column, alias=None) -%}
    {%- set col_name = column -%}
    {%- if env_var('DEPLOYMENT_ENVIRONMENT', '') == 'US Gov' -%}
        {%- if alias -%}
            {%- set col_name = 'NULL AS ' ~ alias -%}
        {%- else -%}
            {%- set col_name = 'NULL AS ' ~ col_name -%}
        {%- endif -%}
    {%- else -%}
        {%- if alias -%}
            {%- set col_name = col_name ~ ' AS ' ~ alias -%}
        {%- else -%}
            {%- set col_name = col_name -%}
        {%- endif -%}
    {%- endif -%}
    {{- col_name -}}
{%- endmacro -%}

{%- macro environment_varchar_to_timestamp(column,alias) -%}
    {%- set col_name = column -%}
    {%- if env_var('DEPLOYMENT_ENVIRONMENT', '') == 'US Gov' -%}
        to_timestamp_ntz({{ col_name }},'YYYY-MM-DDTHH24:MI:SS.FF3+TZHTZM') as {{ alias }}
    {%- else -%}
        {{ load_timestamp(col_name, alias= alias) }}
    {%- endif -%}
{%- endmacro -%}
