{% macro is_latest_complete_day(date_key) -%}

-- used in incremental models in which current day is surfaced only after midnight.

{{ date_key }} = TO_CHAR(CURRENT_DATE - INTERVAL '1 DAY', 'YYYYMMDD')::INT

{%- endmacro %}
