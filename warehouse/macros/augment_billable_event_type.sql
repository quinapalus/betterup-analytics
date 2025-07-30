{% macro augment_billable_event_type(event_type, session_type) -%}

-- generate an augmented classification of an event type by using session type information.

  CASE

    WHEN {{ event_type }} != 'reflection_point' THEN (

      CASE
        WHEN {{ session_type }} = 'primary' THEN event_type || '_primary'
        WHEN {{ session_type }} = 'secondary' THEN event_type || '_extended_network'
        WHEN {{ session_type }} = 'on_demand' THEN event_type || '_on_demand'
      ELSE event_type -- account for instances when session_type is NULL
      END

    )

    ELSE event_type -- when event type is reflection_point

  END

{%- endmacro %}
