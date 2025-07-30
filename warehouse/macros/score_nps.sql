{% macro score_nps(response) -%}

-- convert raw 0-10 point scale response to score that can be aggregated

CASE
  WHEN {{ response }} <= 6 THEN -100
  WHEN {{ response }}  >= 9 THEN 100
  ELSE 0
END

{%- endmacro %}
