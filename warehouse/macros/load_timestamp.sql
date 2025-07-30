{% macro load_timestamp(ts, alias=ts) -%}

{{ ts }}::timestamp_ntz AS {{ alias }}

{%- endmacro %}
