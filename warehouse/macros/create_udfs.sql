{% macro create_udfs() %}

create schema if not exists {{ target.schema }};

{{ f_norm_cdf() }}
{{ array_diff() }}
{{ encode_coach_creation_url() }}

{% endmacro %}
