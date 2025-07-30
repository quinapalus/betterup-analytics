{% macro get_month_difference(first_ts, second_ts, rounding_function='') -%}

-- extract difference in fractional months by converting from seconds
-- rounding_function is an optional parameter to process the numeric result
-- e.g. calling as get_month_difference('first_event_at', 'second_event_at', 'CEIL')
--      would round the floating point result using CEIL. Note that the rounding_function
--      is passed into the macro as a string.

{{ rounding_function }}(DATEDIFF('second', {{ first_ts }}, {{ second_ts }}) / 86400.0 / 30)

{%- endmacro %}
