{%- macro convert_timezone(field, timezone) -%}
     convert_timezone('{{timezone}}', {{field}})::timestamp
{%- endmacro -%}
