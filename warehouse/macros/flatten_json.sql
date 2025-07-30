{%- macro flatten_json(model_name, json_column, rename_airbyte_sfdc_column=false) -%}

{# getting the column names from json/variant column #}

{%- set json_column_query -%}

select 
    json.key as column_name,
    typeof(json.value) as data_type
from {{ model_name }},
lateral flatten(input=>{{ json_column }}) as json
where json.value is not null and typeof(json.value) != 'NULL_VALUE'
--Once the airbyte sfdc column renamer runs there are some duplicate columns created.
--The where clause below removes the duplicate columns from the list of columns
--In segment, it appears that one column is chosen and the other is simply not made available
--It is unclear how Segment makes that determination. Fortunately, none of the duplicate columns that segment
--does not select are needed for our reporting
{%- if rename_airbyte_sfdc_column==true %} 
and column_name not in ('AccountRegion__c','CareRegion__c','LastReferencedDate','LastViewedDate')
{%- endif -%}
--qualify clause is setting the grain at one row per column prioritizing the non null values first
qualify row_number() over(partition by json.key order by json.value asc) = 1
{%- endset -%}
{# create a table object with the results from json_column_query #}
{%- set results = run_query(json_column_query) -%}
{%- if execute -%}
    {# creating list of the column names #}
    {%- set column_names = results.columns[0].values() -%}
    {# creating list of the column data types #}
    {%- set column_data_types = results.columns[1].values() -%}
    {# creating list of tuples with column names and data types #}

    {%- set column_detail = zip(column_names, column_data_types) | list -%}

{% else %}
    {% set column_detail = []  %}
{% endif %}

select
    {{ json_column }},
    {# if this is for an Airbyte Salesforce table we need to get the lastmodifieddate and re-alias it to uuid_ts to match segment timestamp
       this is used in the updated_at parameter in snapshots. #}
    {%- if rename_airbyte_sfdc_column==true %}
    {{ json_column }}:"LastModifiedDate"::varchar as uuid_ts,
    
    {# Airbyte is using using an older version of the Salesforce API which means that the below columns are available in Segment but not in Airbyte
     Fortunately, we dont use these columns in reporting however there are some references in looker views so keeping them here for now but just realiasing with
    last_modified_date #}
    {{ json_column }}:"LastModifiedDate"::varchar as last_referenced_date,
    {{ json_column }}:"LastModifiedDate"::varchar as last_viewed_date,

    {% endif %}
    {# for each column name in column_detail select the column, alias it as the column_name and set the datatype #}
    {% for name, data_type in column_detail %}
    {{ json_column }}:{{ name }}::{{ data_type }} as 
    {% if rename_airbyte_sfdc_column==true %} 
    {{ convert_airbyte_sfdc_name_to_segment_sfdc_name(name) }} 
    {% else %} {{ name }} 
    {% endif %}  {% if not loop.last %}, {% endif %}
    {% endfor %}

from {{ model_name }}

{% endmacro %}
