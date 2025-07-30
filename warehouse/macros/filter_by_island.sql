{% macro filter_by_island(account_env) %}

{%- if env_var('DEPLOYMENT_ENVIRONMENT', '') == 'US Prod' -%}
    island = 'us'
{%- elif env_var('DEPLOYMENT_ENVIRONMENT', '') == 'EU Prod' -%}
    island = 'eu'
{%- elif env_var('DEPLOYMENT_ENVIRONMENT', '') == 'US Gov' -%}
    island = 'gov'
{%- else -%}
    island = 'us' -- setting US as default.
{% endif %}

{% endmacro %}
