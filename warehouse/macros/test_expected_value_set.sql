{% macro test_expected_value_set(model, values) %}

{% set column_name = kwargs.get('column_name', kwargs.get('field')) %}

with expected_value_set as (

    
  {% for value in values %}
    select 
     '{{ value }}' as value {% if not loop.last -%} union all {% endif %}

  {% endfor %}
),

validation_errors as (
  -- find actual values not present in expected values, and expected values
  -- not present in actual values
  select distinct
    actual_value_set.{{ column_name }},
    expected_value_set.value
  from {{ model }} as actual_value_set
  full outer join expected_value_set
    on expected_value_set.value = actual_value_set.{{ column_name }}
  where actual_value_set.{{ column_name }} is null
     or expected_value_set.value is null
)

select count(*)
from validation_errors

{% endmacro %}
