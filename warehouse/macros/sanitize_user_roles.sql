{% macro sanitize_user_roles(roles) %}

-- Assign a single user role using sequential WHEN statements
-- to order based on priority.

CASE
  WHEN array_contains('partner'::variant, {{ roles }}) = true 
    THEN 'Partner'
  WHEN array_contains('coach'::variant, {{ roles }}) = true
    THEN 'Coach'
  WHEN array_contains ('member'::variant, {{ roles }}) = true
    THEN 'Member'
  ELSE 'Other'
END

{% endmacro %}
