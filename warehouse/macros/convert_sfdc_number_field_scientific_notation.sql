{% macro convert_sfdc_number_field_scientific_notation(column_name) -%}

--The Salesforce API converts numeric fields that have really large values to scientific notation and changes them to varchar.
--This macro will convert a column that Salesforce is doing to back to a numeric datatype 
--Salesforce documentation on this behavior https://help.salesforce.com/s/articleView?id=000385889&type=1

ltrim(rtrim(
    case 
        when {{ column_name }} like '%E%' 
            then cast(cast({{ column_name }} as float) as decimal)
        else {{ column_name }}
    end))

{%- endmacro %}
