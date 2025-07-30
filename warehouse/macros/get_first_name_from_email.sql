{% macro get_first_name_from_email(email) -%}
-- separate out the @ domain part of the email and get the first part before .
    initcap(split_part(split_part({{email}},'@',1),'.',1))

{%- endmacro %}