{%- macro convert_airbyte_sfdc_name_to_segment_sfdc_name(airbyte_column_name) -%}

{# rare or one-off naming exceptions will be handled using the below dictionary loop #}

{% set exceptions = {
       
        'mkto71_Lead_Score__c':'mkto_71_lead_score_c',
        'Product2Id':'product_2_id'
} %}

{% for key,value in exceptions.items() %}
    {%- if key == airbyte_column_name -%}
        {{ return(value) }}
    {%- endif -%}
{% endfor %}

{%- set re = modules.re -%}
{%- set find_pattern = "([a-z0-9])([A-Z])" -%}
{%- set replace_pattern = "\\1_\\2" -%}
{%- set result = re.sub(find_pattern, replace_pattern, airbyte_column_name) -%}
{%- set partitions = result.replace('__', '_').split('_') -%}
{%- set result = [] -%}
{%- for partition in partitions -%}
    {%- set result = result.append(partition.lower()) -%}
{%- endfor -%}
{%- set result = result | join('_') -%}

{# This handles scenarios where the column name starts with X then has other conssective numbers later in the string #}
{%- if result.startswith('x') and result[1] in '0123456789' -%}
    {%- set next_index = -1 -%}
    {%- for i in range(2, result|length) -%}
        {%- set next_index = i if result[i] not in '0123456789' and next_index == -1 else next_index -%}
    {%- endfor -%}
    {%- if next_index != -1 -%}
        {%- set result = result[0:next_index] + '_' + result[next_index:] -%}
    {%- endif -%}
{%- endif -%}

{# Add underscore between a digit and a lowercase letter followed by an uppercase letter #}
{%- set find_pattern2 = "([0-9])([a-z])([A-Z])" -%}
{%- set replace_pattern2 = "\\1_\\2\\3" -%}
{%- set result = re.sub(find_pattern2, replace_pattern2, result) -%}

{# Add underscore between two uppercase letters followed by a lowercase letter #}
{%- set find_pattern3 = "([A-Z])([A-Z])([a-z])" -%}
{%- set replace_pattern3 = "\\1_\\2\\3" -%}
{%- set result = re.sub(find_pattern3, replace_pattern3, result) -%}

{# if a salesforce column starts with a number segment appends an x to the beginning of the column name and then does x1_ + rest_of_field name
below logic handles that scenario #}
{%- if result.startswith('x') and result[1] in '0123456789'
and result[2] not in '0123456789' -%}
{%- set result = result[0] + result[1] + '_' + result[2:] -%}
{%- endif -%}

{{ return(result) }}

{%- endmacro -%}
