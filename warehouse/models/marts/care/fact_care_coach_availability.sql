WITH app_coach_recommendation_sets AS (

  SELECT * FROM {{ref('stg_app__coach_recommendation_sets')}}

),

fact_member_events AS (

  SELECT * FROM {{ref('fact_member_events')}}

),

fact_member_events AS (

  SELECT * FROM {{ref('fact_member_events')}}

),

app_product_subscription_assignments AS (

  SELECT * FROM {{ref('int_app__product_subscription_assignments')}}

),

app_coach_recommendations AS (

  SELECT * FROM {{ref('stg_app__coach_recommendations')}}

),


stg_app__timeslots as ( 

  SELECT * FROM {{ref('stg_app__timeslots')}}

),

-- Connect all members that have "activated care_user" with their recommendation sets
recommendation_sets
    as (
        select 
            distinct
                e.member_id::int as member_id,
                e.event_at as activated_care_at,
                rs.coach_recommendation_set_id as coach_recommendation_set_id,
                rs.created_at as coach_recommendation_set_created_at,
                dense_rank() over (partition by e.member_id, e.event_at order by rs.created_at) as coach_recommendation_set_sequence
         from
             fact_member_events e
             join app_product_subscription_assignments sa
                  on e.associated_record_id = sa.product_subscription_assignment_id
                      and e.associated_record_type = 'ProductSubscriptionAssignment'
             join app_coach_recommendation_sets rs
                  on e.member_id = rs.member_id
                      and rs.coaching_type = 'care'
                      and rs.created_at > sa.starts_at
                      and rs.created_at < sa.ends_at
                      and rs.created_at > e.event_at -- make sure the recommendations happens after the user is activated as a care user
         where
               e.event_name = 'activated care_user'
         ), 

-- filter for FIRST recommendation set and connect set to the coaches
recommendations_detailed
    as (
        select 
            distinct
                rs.member_id,
                rs.activated_care_at,
                first_value(e.event_at)
                                over (partition by e.member_id order by e.event_at)
                    as first_completed_care_appointment_at,
                datediff(days, rs.activated_care_at, first_completed_care_appointment_at)
                    as days_between_activation_and_first_session,
                rs.coach_recommendation_set_id,
                rs.coach_recommendation_set_created_at,
                cr.coach_id,
                t.created_at as timeslot_created_at,
                t.starts_at as timeslot_starts_at,
                t.ends_at as timeslot_ends_at,
                t.updated_at as timeslot_updated_at,
                datediff(days, rs.coach_recommendation_set_created_at, t.starts_at)
                    as days_until_available_slot,
                case
                        when available_minutes = 0 and t.updated_at > cr.created_at and t.created_at < cr.created_at
                            then datediff(minutes, t.starts_at, t.ends_at)
                        when t.updated_at < cr.created_at then available_minutes
                    end
                    as coach_timeslot_available_minutes
         from
             recommendation_sets rs
             join app_coach_recommendations cr
                  on rs.coach_recommendation_set_id = cr.coach_recommendation_set_id
             left join fact_member_events e
                       on rs.member_id = e.member_id and e.event_name = 'completed care_appointment'
             left join stg_app__timeslots t on cr.coach_id = t.coach_id
                 and datediff(days, rs.coach_recommendation_set_created_at, t.starts_at) <= 30
                 and t.starts_at > rs.coach_recommendation_set_created_at
                 and t.created_at < rs.coach_recommendation_set_created_at
         where
               rs.coach_recommendation_set_sequence = 1
           and cr.position <= 3 -- where prospective coach position is 3 and less
         ), 

-- filter out the recommendations that don't have any available minutes
recommendations_filtered
    as (
        select
            *
        from recommendations_detailed
        where coach_timeslot_available_minutes > 0
        ), 

-- check if any timeslots are overlapping
overlapping_check
    as (
        select distinct
          sr.*,
          sr2.timeslot_starts_at as overlapping_start,
          sr2.timeslot_ends_at as overlapping_end,
          sr2.timeslot_updated_at as overlapping_updated_at
        from recommendations_filtered sr
             left join recommendations_filtered sr2
                       on sr.member_id = sr2.member_id
                           and sr2.timeslot_starts_at < sr.timeslot_ends_at
                           and sr2.timeslot_ends_at > sr.timeslot_starts_at
                           and sr2.timeslot_created_at > sr.timeslot_created_at
        ),

-- remove any overlapping timeslots and report on various details and metrics such as half_hour_avilable_slot and days_until_available_slot
finalize_before_surrogate_key
    as ( 
        select 
            distinct
               member_id,
               activated_care_at,
               first_completed_care_appointment_at,
               days_between_activation_and_first_session,
               coach_id,
               coach_timeslot_available_minutes,
               round(coach_timeslot_available_minutes/30,1) as half_hour_available_slot,
               days_until_available_slot,
               timeslot_starts_at,
               timeslot_ends_at,
               coach_recommendation_set_id,
               coach_recommendation_set_created_at,
               rank() over (partition by member_id order by timeslot_starts_at, coach_id) as available_sessions_ranked,
               case
                    when
                            dense_rank() over (partition by member_id order by timeslot_starts_at, coach_id) = 1
                        then datediff(days, coach_recommendation_set_created_at, timeslot_starts_at)
                end as days_until_first_session_available
             from
                 overlapping_check
             where
                   overlapping_start is null
               and first_completed_care_appointment_at is not null
               and days_until_available_slot <= 14
    ) 

    select
        {{ dbt_utils.surrogate_key(['member_id', 'coach_id', 'coach_recommendation_set_id', 'timeslot_starts_at', 'timeslot_ends_at']) }} AS primary_key,  
        * 
    from finalize_before_surrogate_key