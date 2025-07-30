{% macro sanitize_nps_response_category(response) -%}

-- map NPS response to Promoter/Neutral/Detractor

CASE
  WHEN {{ response }} IN (9, 10) THEN 'Promoter'
  WHEN {{ response }} IN (7, 8) THEN 'Neutral'
  ELSE 'Detractor'
END

{%- endmacro %}
