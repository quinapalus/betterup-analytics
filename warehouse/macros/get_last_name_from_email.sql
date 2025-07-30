{% macro get_last_name_from_email(email) -%}
-- if email contains . return the second part as the last name
-- else return first part after removing the @ domain
CASE WHEN contains(split_part({{email}},'@',1),'.')
     THEN split_part(split_part({{email}},'@',1),'.',2)
     ELSE split_part(split_part({{email}},'@',1),'.',1)
     END

{%- endmacro %}