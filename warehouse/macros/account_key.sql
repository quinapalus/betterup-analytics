{% macro account_key(app_organization_id, sfdc_account_id) -%}

{{ dbt_utils.surrogate_key([app_organization_id, sfdc_account_id]) }}

{%- endmacro %}
