{% macro convert_yes_no_string_to_boolean (column_name) %}

case
    when upper({{ column_name }}) = 'YES'
        then true
    when upper({{ column_name }}) = 'NO'
        then false
    else null end 

{%- endmacro %}

--There are many Salesforce picklist fields where the only options are "Yes" and "No"
--It is unclear why they are created like this instead of as boolean true/false. 
--This macro does the conversion from 'Yes' and 'No' to true/false
--If we have other sources with fields like this we can use this macro for those
