{%- macro quoted_list(list) -%}
{# this macro is used to convert a list of strings into a comma separated list of quoted strings for use in SQL "in" statements  #}
    {%- set result = [] -%}
    {%- for item in list -%}
        {%- do result.append("'" ~ item ~ "'") -%}
    {%- endfor -%}
    {{ result | join(", ") }}
{%- endmacro -%}
