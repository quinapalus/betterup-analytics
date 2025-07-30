{% macro word_count(string) -%}

-- trim leading and trailing whitespace, count remaining white space characters
-- and add 1 for fencepost error
(REGEXP_COUNT(TRIM({{ string }}), '\\s+') + 1)

{%- endmacro %}
