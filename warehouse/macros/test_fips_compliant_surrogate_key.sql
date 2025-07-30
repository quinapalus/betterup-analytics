{% macro test_surrogate_key() %}
    {% set result = dbt_utils.surrogate_key(['dummy_field']) %}
    {% if result.strip()[0:3] != 'md5' %}
        {{ exceptions.raise_compiler_error('Test failed: Regular surrogate_key function did not use md5') }}
    {% endif %}
{% endmacro %}

{% macro test_patched_surrogate_key() %}
    {% set result = dbt_utils.surrogate_key(['dummy_field']) %}
    {% if result.strip()[0:4] != 'sha2' %}
        {{ exceptions.raise_compiler_error('Test failed: patched surrogate_key function did not use SHA256') }}
    {% endif %}
{% endmacro %}
