{% macro is_coach_cost_manual_upload(event_type) -%}

  -- classify whether a coach cost associated with an event type is a manual upload,
  -- defined by whether the finance team needs to manually upload this cost data.
  CASE
    WHEN event_type IN ('training_stipend', 'training_session', 'launch_call', 'incentive', 'miscellaneous', 'event_adjustments') THEN true
    ELSE false
  END

{%- endmacro %}
