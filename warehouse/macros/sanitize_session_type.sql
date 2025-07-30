{% macro sanitize_session_type(session_type) -%}

-- define user-friendly naming conventions for coaching type in sessions.

CASE
  WHEN {{ session_type }} = 'primary' THEN 'Primary'
  WHEN {{ session_type }} = 'secondary' THEN 'Extended Network'
  WHEN {{ session_type }} = 'on_demand' THEN 'On Demand'
END

{%- endmacro %}
