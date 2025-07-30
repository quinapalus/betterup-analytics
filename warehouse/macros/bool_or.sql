{%- macro bool_or(field) -%}
    {{return('MAX')}}
{%- endmacro -%}

{%- macro bool_and(field) -%}
    {{return('MIN')}}
{%- endmacro -%}
