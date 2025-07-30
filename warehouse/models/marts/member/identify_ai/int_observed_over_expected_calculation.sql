{{ config(
    tags=["identify_ai_metrics"],
) }}

WITH all_events_avg_sessions AS (

  SELECT * FROM {{ref('int_wle_weights_calculation')}}

),

--observed over expected = OE
--OE = avg # of sessions with event / avg of (avg # of sessions of all other events)
oe_calculation as(

      SELECT wle, avg_event_sessions/(SELECT avg(avg_event_sessions) FROM all_events_avg_sessions) as observed_over_expected
      FROM all_events_avg_sessions

)

SELECT * FROM oe_calculation
