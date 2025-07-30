WITH
fact_coach_compensation_events_seasoned_base AS (
    SELECT * FROM {{ ref('fact_coach_compensation_events_seasoned') }}
),
dim_coach_compensation_seasons AS (
    SElECT * FROM {{ ref('dim_coach_compensation_seasons') }}
),
dim_coaches AS (
    SELECT * FROM {{ ref('dim_coaches') }}
),
fact_coach_compensation_events_seasoned AS (
  select
          events.primary_key,
          events.associated_record_id,
          seasons.season_id,
          events.coach_id,
          events.event_at,
          events.event_action_and_object,
          events.attributes,
            case when
                event_action_and_object = 'completed coaching session'
                and attributes:coaching_assignment_role = 'primary'
                and attributes:deployment_group in ('B2B / Gov Paid Contract','BetterUp')
                and attributes:is_professional_coaching_cloud = 1
                and (attributes:minutes_limit > 900 or attributes:minutes_limit is null)
            then 1 else null end as primary_b2b_session_sequence_flag,
          case when
                primary_b2b_session_sequence_flag = 1
            then
            row_number() over(
              partition by
              primary_b2b_session_sequence_flag,attributes:coach_assignment_id,season_name
              order by event_at asc) end as  primary_b2b_completed_coach_assignment_session_sequence,
          seasons.season_start_date,
          seasons.season_end_date
         from fact_coach_compensation_events_seasoned_base events
          left join dim_coach_compensation_seasons seasons
            on seasons.season_id = events.season_id
),

final as (SELECT
    dim_coaches.coach_id  AS coach_id,
    dim_coaches."FULL_NAME"  AS full_name,
    dim_coach_compensation_seasons.season_name  AS season_name,
    floor(( COUNT(DISTINCT case
      when
        (((fact_coach_compensation_events_seasoned.attributes:coaching_assignment_role) = 'primary'
        and (fact_coach_compensation_events_seasoned.attributes:deployment_group) in ('B2B / Gov Paid Contract','BetterUp')
        and (fact_coach_compensation_events_seasoned.attributes:is_professional_coaching_cloud = 1))

        or

        ((fact_coach_compensation_events_seasoned.attributes:specialist_vertical_uuid) = 'c8db04ad-3d63-4b23-bef3-fa32258a4824') --this was a specialist vertial created for the google launch. Coach ops wants these included in lifetime sessions and LCA
)
        and fact_coach_compensation_events_seasoned.event_action_and_object = 'submitted post session assessment'
        and fact_coach_compensation_events_seasoned.event_at >= fact_coach_compensation_events_seasoned.season_start_date
        and fact_coach_compensation_events_seasoned.event_at < fact_coach_compensation_events_seasoned.season_end_date
        and (fact_coach_compensation_events_seasoned.attributes:assessment_response_score) >= 4
      then fact_coach_compensation_events_seasoned.associated_record_id end) )/nullif(( COUNT(DISTINCT case
      when
        (((fact_coach_compensation_events_seasoned.attributes:coaching_assignment_role) = 'primary'
        and (fact_coach_compensation_events_seasoned.attributes:deployment_group) in ('B2B / Gov Paid Contract','BetterUp')
        and (fact_coach_compensation_events_seasoned.attributes:is_professional_coaching_cloud = 1))

        or

        ((fact_coach_compensation_events_seasoned.attributes:specialist_vertical_uuid) = 'c8db04ad-3d63-4b23-bef3-fa32258a4824') --this was a specialist vertial created for the google launch. Coach ops wants these included in lifetime sessions and LCA
)
        and fact_coach_compensation_events_seasoned.event_action_and_object = 'submitted post session assessment'
        and fact_coach_compensation_events_seasoned.event_at >= fact_coach_compensation_events_seasoned.season_start_date
        and fact_coach_compensation_events_seasoned.event_at < fact_coach_compensation_events_seasoned.season_end_date
        and (fact_coach_compensation_events_seasoned.attributes:assessment_response_score) >= 1
      then fact_coach_compensation_events_seasoned.associated_record_id end) ),0),2) AS primary_b2b_post_session_assessment_lca_rate,
    COUNT(DISTINCT case
      when
        ( ((fact_coach_compensation_events_seasoned.attributes:coaching_assignment_role) = 'primary'
        and (fact_coach_compensation_events_seasoned.attributes:deployment_group) in ('B2B / Gov Paid Contract','BetterUp')
        and (fact_coach_compensation_events_seasoned.attributes:is_professional_coaching_cloud = 1))

        or

        ((fact_coach_compensation_events_seasoned.attributes:specialist_vertical_uuid) = 'c8db04ad-3d63-4b23-bef3-fa32258a4824') --this was a specialist vertial created for the google launch. Coach ops wants these included in lifetime sessions and LCA
         )
        and  fact_coach_compensation_events_seasoned.event_action_and_object   = 'submitted post session assessment'
        and  fact_coach_compensation_events_seasoned.event_at   >=  fact_coach_compensation_events_seasoned.season_start_date
        and  fact_coach_compensation_events_seasoned.event_at   <  fact_coach_compensation_events_seasoned.season_end_date
        and ( fact_coach_compensation_events_seasoned.attributes:assessment_response_score  ) >= 1
      then  fact_coach_compensation_events_seasoned.associated_record_id  end) AS primary_b2b_post_session_assessments,
    floor(( COUNT(DISTINCT case
      when
        (dateadd('day',90,(fact_coach_compensation_events_seasoned.attributes:first_completed_primary_b2b_coach_assignment_session_date))) >= fact_coach_compensation_events_seasoned.season_start_date
        and (dateadd('day',90,(fact_coach_compensation_events_seasoned.attributes:first_completed_primary_b2b_coach_assignment_session_date))) < fact_coach_compensation_events_seasoned.season_end_date
        and (dateadd('day',90,(fact_coach_compensation_events_seasoned.attributes:first_completed_primary_b2b_coach_assignment_session_date))) <= (fact_coach_compensation_events_seasoned.attributes:primary_b2b_member_access_ended_date)
        and fact_coach_compensation_events_seasoned.event_action_and_object = 'completed coaching session'
        and fact_coach_compensation_events_seasoned.primary_b2b_completed_coach_assignment_session_sequence = 5
        and (dateadd('day',90,(fact_coach_compensation_events_seasoned.attributes:first_completed_primary_b2b_coach_assignment_session_date))) <= current_date
        and fact_coach_compensation_events_seasoned.event_at <= current_date
        and (fact_coach_compensation_events_seasoned.attributes:days_since_first_completed_primary_b2b_coach_assignment_session) < 90
      then fact_coach_compensation_events_seasoned.associated_record_id end) )/nullif(( COUNT(DISTINCT case
      when
        (dateadd('day',90,(fact_coach_compensation_events_seasoned.attributes:first_completed_primary_b2b_coach_assignment_session_date))) >= fact_coach_compensation_events_seasoned.season_start_date
        and (dateadd('day',90,(fact_coach_compensation_events_seasoned.attributes:first_completed_primary_b2b_coach_assignment_session_date))) < fact_coach_compensation_events_seasoned.season_end_date
        and (dateadd('day',90,(fact_coach_compensation_events_seasoned.attributes:first_completed_primary_b2b_coach_assignment_session_date))) <= (fact_coach_compensation_events_seasoned.attributes:primary_b2b_member_access_ended_date)
        and fact_coach_compensation_events_seasoned.event_action_and_object = 'completed coaching session'
        and fact_coach_compensation_events_seasoned.primary_b2b_completed_coach_assignment_session_sequence = 1
        and (dateadd('day',90,(fact_coach_compensation_events_seasoned.attributes:first_completed_primary_b2b_coach_assignment_session_date))) <= current_date
      then fact_coach_compensation_events_seasoned.associated_record_id end) ),0),2) AS primary_b2b_five_in_90_rate,
    COUNT(DISTINCT case
      when
        ( dateadd('day',90,(fact_coach_compensation_events_seasoned.attributes:first_completed_primary_b2b_coach_assignment_session_date)) ) >=  fact_coach_compensation_events_seasoned.season_start_date
        and ( dateadd('day',90,(fact_coach_compensation_events_seasoned.attributes:first_completed_primary_b2b_coach_assignment_session_date)) ) <  fact_coach_compensation_events_seasoned.season_end_date
        and ( dateadd('day',90,(fact_coach_compensation_events_seasoned.attributes:first_completed_primary_b2b_coach_assignment_session_date)) ) <= ( fact_coach_compensation_events_seasoned.attributes:primary_b2b_member_access_ended_date )
        and  fact_coach_compensation_events_seasoned.event_action_and_object   = 'completed coaching session'
        and  fact_coach_compensation_events_seasoned.primary_b2b_completed_coach_assignment_session_sequence   = 1
        and ( dateadd('day',90,(fact_coach_compensation_events_seasoned.attributes:first_completed_primary_b2b_coach_assignment_session_date)) ) <= current_date
      then  fact_coach_compensation_events_seasoned.associated_record_id  end) AS primary_b2b_5_in_90_first_time_sessions,
    COUNT(DISTINCT case
      when
         fact_coach_compensation_events_seasoned.event_action_and_object   = 'completed billable event'
        and ( fact_coach_compensation_events_seasoned.attributes:billing_event_type  ) in ('completed_sessions','completed_group_sessions')
        and  fact_coach_compensation_events_seasoned.event_at   <=  fact_coach_compensation_events_seasoned.season_end_date
      then  fact_coach_compensation_events_seasoned.associated_record_id  end) AS lifetime_sessions,
    COUNT(DISTINCT case
      when
        ( fact_coach_compensation_events_seasoned.event_action_and_object   = 'completed billable event'
        and ( fact_coach_compensation_events_seasoned.attributes:billing_event_type  ) in ('completed_sessions')
        and ( fact_coach_compensation_events_seasoned.attributes:coaching_assignment_role  ) = 'primary'
        and ( fact_coach_compensation_events_seasoned.attributes:deployment_group  ) in ('B2B / Gov Paid Contract','BetterUp')
        and ( fact_coach_compensation_events_seasoned.attributes:is_professional_coaching_cloud = 1  )
        and  fact_coach_compensation_events_seasoned.event_at   <=  fact_coach_compensation_events_seasoned.season_end_date )

        or

        ( fact_coach_compensation_events_seasoned.event_action_and_object   = 'completed billable event'
        and ( fact_coach_compensation_events_seasoned.attributes:billing_event_type  ) in ('completed_sessions')
        and ( fact_coach_compensation_events_seasoned.attributes:specialist_vertical_uuid  ) = 'c8db04ad-3d63-4b23-bef3-fa32258a4824' --this was a specialist vertial created for the google launch. Coach ops wants these included in lifetime sessions and LCA
        and  fact_coach_compensation_events_seasoned.event_at   <=  fact_coach_compensation_events_seasoned.season_end_date )

      then  fact_coach_compensation_events_seasoned.associated_record_id  end) AS lifetime_primary_b2b_sessions,
    COUNT(DISTINCT case
      when
        ( dateadd('day',90,(fact_coach_compensation_events_seasoned.attributes:first_completed_primary_b2b_coach_assignment_session_date)) ) >=  fact_coach_compensation_events_seasoned.season_start_date
        and ( dateadd('day',90,(fact_coach_compensation_events_seasoned.attributes:first_completed_primary_b2b_coach_assignment_session_date)) ) <  fact_coach_compensation_events_seasoned.season_end_date
        and ( dateadd('day',90,(fact_coach_compensation_events_seasoned.attributes:first_completed_primary_b2b_coach_assignment_session_date)) ) <= ( fact_coach_compensation_events_seasoned.attributes:primary_b2b_member_access_ended_date )
        and  fact_coach_compensation_events_seasoned.event_action_and_object   = 'completed coaching session'
        and  fact_coach_compensation_events_seasoned.primary_b2b_completed_coach_assignment_session_sequence   = 5
        and ( dateadd('day',90,(fact_coach_compensation_events_seasoned.attributes:first_completed_primary_b2b_coach_assignment_session_date)) ) <= current_date
        and  fact_coach_compensation_events_seasoned.event_at   <= current_date
        and ( fact_coach_compensation_events_seasoned.attributes:days_since_first_completed_primary_b2b_coach_assignment_session  ) < 90
      then  fact_coach_compensation_events_seasoned.associated_record_id  end) AS primary_b2b_five_in_90_sessions,
    COUNT(DISTINCT case
      when
        ( ((fact_coach_compensation_events_seasoned.attributes:coaching_assignment_role) = 'primary'
        and (fact_coach_compensation_events_seasoned.attributes:deployment_group) in ('B2B / Gov Paid Contract','BetterUp')
        and (fact_coach_compensation_events_seasoned.attributes:is_professional_coaching_cloud = 1))

        or

        ((fact_coach_compensation_events_seasoned.attributes:specialist_vertical_uuid) = 'c8db04ad-3d63-4b23-bef3-fa32258a4824') --this was a specialist vertial created for the google launch. Coach ops wants these included in lifetime sessions and LCA
         )
        and  fact_coach_compensation_events_seasoned.event_action_and_object   = 'submitted post session assessment'
        and  fact_coach_compensation_events_seasoned.event_at   >=  fact_coach_compensation_events_seasoned.season_start_date
        and  fact_coach_compensation_events_seasoned.event_at   <  fact_coach_compensation_events_seasoned.season_end_date
        and ( fact_coach_compensation_events_seasoned.attributes:assessment_response_score  ) >= 4
      then  fact_coach_compensation_events_seasoned.associated_record_id  end) AS primary_b2b_post_session_lca_assessments
FROM dim_coaches AS dim_coaches
LEFT JOIN dim_coach_compensation_seasons AS dim_coach_compensation_seasons ON 1=1
LEFT JOIN fact_coach_compensation_events_seasoned ON fact_coach_compensation_events_seasoned.season_id = dim_coach_compensation_seasons.season_id
      and fact_coach_compensation_events_seasoned.coach_id = dim_coaches.coach_id
WHERE  (dim_coaches."STAFFABLE_STATE" ) IN ('hold_involuntary', 'hold_voluntary', 'staffable')

GROUP BY
    1,
    2,
    3
order by 1)

select
coach_id
, full_name
, season_name
, coalesce(primary_b2b_post_session_assessment_lca_rate, 0) as primary_b2b_lca_rate
, coalesce(primary_b2b_post_session_assessments, 0) as primary_b2b_assessments
, coalesce(primary_b2b_five_in_90_rate, 0) as primary_b2b_5_in_90_rate
, coalesce(primary_b2b_5_in_90_first_time_sessions, 0) as primary_b2b_matches
, coalesce(lifetime_sessions, 0) as lifetime_sessions
, coalesce(lifetime_primary_b2b_sessions, 0) as primary_b2b_lifetime_sessions
, coalesce(primary_b2b_five_in_90_sessions, 0) as primary_b2b_5_in_90_sessions
, coalesce(primary_b2b_post_session_lca_assessments, 0) as primary_b2b_lca_assessments
from final
