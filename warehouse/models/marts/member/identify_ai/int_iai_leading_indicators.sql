{{ config(
    tags=["identify_ai_metrics"],
) }}


WITH members_ending_in_next30days AS (

  SELECT * FROM {{ref('int_iai_members')}}

)

-- Average # session hours per month, averaged over last 2 months
, avg_session_hours AS (

  select a.member_id, sum(appointment_length)/2.0/60.0 as avg_session_hours_last60days
  from {{ref('stg_app__appointments')}} a
  join members_ending_in_next30days m on a.member_id = m.member_id
  where complete_at is not null
  and complete_at BETWEEN DATEADD(DAY,-60,GETDATE()) AND GETDATE()
  group by 1

)

--instead of just counting WLEs, now we want to get a list of WLEs per member and get the WLE weight(ie, observed_over_expected) associated with that.
--variable wle_weights_calculation
, variable_wle as(

  select user_id, work_life_event, oe.observed_over_expected as weighted_work_life_events
  from members_ending_in_next30days m
  join {{ref('stg_app__assessments')}} a on m.member_id = a.user_id
  join {{ref('assessments__assessment_text_scores')}} ts on ts.assessment_id = a.assessment_id
  join {{ref('int_observed_over_expected_calculation')}} oe on oe.wle = ts.work_life_event
  where a.submitted_at BETWEEN DATEADD(DAY,-30,GETDATE()) AND GETDATE()
  and work_life_event is not null
  and work_life_event != 'No Event'

)

--instead of just counting WLEs, we'll now sum up the weights associated with those WLEs
, sum_wle_weights as (

  select user_id, sum(weighted_work_life_events) as sum_work_life_events_weights
  from variable_wle
  GROUP BY user_id

),

final as (

  SELECT
    me.member_id,
    me.reporting_group_id,
    avg_session_hours_last60days AS avg_session_hours_last60days,
    sum_work_life_events_weights AS variable_work_life_events
  FROM members_ending_in_next30days AS me
  LEFT JOIN avg_session_hours AS ash
  ON me.member_id = ash.member_id
  LEFT JOIN sum_wle_weights AS cwles
  ON me.member_id = cwles.user_id

)

select
  final.*,
  {{ dbt_utils.surrogate_key(['member_id', 'reporting_group_id']) }} as member_reporting_group_id
from final
