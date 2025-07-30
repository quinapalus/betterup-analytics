{% macro score_session_overall(response) -%}

-- convert qualitative answers to 'How was session overall?' to 1-5 scale.

CASE
  WHEN {{ response }} = 'Life Changing' THEN 5
  WHEN {{ response }} = 'Amazing' THEN 4
  WHEN {{ response }} = 'Good' THEN 3
  WHEN {{ response }} = 'Okay' THEN 2
  WHEN {{ response }} = 'Not Great' THEN 1
  ELSE NULL -- In case the answer is 'I'm Not Sure'
END

{%- endmacro %}
