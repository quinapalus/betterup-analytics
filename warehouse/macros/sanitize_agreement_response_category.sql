{% macro sanitize_agreement_response_category(response) -%}

-- map 5 point response scale to Agree/Neutral/Disagree categories

CASE
  WHEN {{ response }} IN (1, 2) THEN 'Disagree and Strongly Disagree'
  WHEN {{ response }} IN (3) THEN 'Neutral'
  WHEN {{ response }} IN (4, 5) THEN 'Agree and Strongly Agree'
END

{%- endmacro %}
