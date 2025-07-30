{% macro priority_language(ranked_language, staffing_languages) -%}

-- If no priority language rank any non-English language as Other
-- above English-only:
COALESCE({{ ranked_language }},
         CASE
           WHEN {{ staffing_languages }} != to_array('en') THEN 'Other'
           WHEN {{ staffing_languages }} = to_array('en') THEN 'en'
           ELSE 'Unknown'
         END)

{%- endmacro %}
