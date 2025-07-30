{{
  config(
    materialized='table'
  )
}}

WITH manager_feedback_flows AS (

  SELECT * FROM {{ref('stg_app__manager_feedback_flows')}}
--This table was last edited on November 1, 2022. There is an edge case when the feedback_request_assessment_id 
--is null for a given manager, member, track_assignment, and assessment_id.The qualify statement below prevents 
--this error from leaking into production runs.). The record that is duplicated in this table is unique on the 
---manager_feedback_flows.manager_feedback_assessment_id column. Grab the most recent record. This statement removes
-- 1 record as of April 14, 2023. 
  qualify(row_number() over (partition by member_id, 
                                          manager_id, 
                                          track_assignment_id, 
                                          feedback_request_assessment_id order by created_at desc) = 1)
),

 assessments AS (

  SELECT * FROM {{ref('stg_app__assessments')}}

),

 track_assignments AS (

  SELECT * FROM {{ref('stg_app__track_assignments')}}

),

 dei_member_engagement_by_track AS (

  SELECT * FROM {{ref('dei_member_engagement_by_track')}}

),

 feedback_assessments AS ( 

  select
  --manager feedback flow attributes
    manager_feedback_flows.member_id,
    manager_feedback_flows.manager_id,
	  manager_feedback_flows.track_assignment_id,
	  manager_feedback_flows.feedback_request_assessment_id,
	  manager_feedback_flows.manager_feedback_assessment_id as flow_manager_feedback_assessment_id,
	  req_ass.submitted_at as feedback_request_submitted_at,

    --aggregation
	  min(feed_ass.submitted_at) as manager_feedback_assessment_submitted_at
  from manager_feedback_flows
  left join assessments as req_ass
    	on manager_feedback_flows.feedback_request_assessment_id = req_ass.assessment_id
  left join assessments as feed_ass
      on manager_feedback_flows.manager_id = feed_ass.creator_id
        and manager_feedback_flows.member_id = feed_ass.user_id
          and feed_ass.submitted_at > req_ass.submitted_at
          and feed_ass.type = 'Assessments::ManagerFeedbackAssessment'
          and (manager_feedback_flows.track_assignment_id = feed_ass.track_assignment_id 
                or  manager_feedback_flows.track_assignment_id is null 
                or feed_ass.track_assignment_id is null
                )
  group by 1, 2, 3, 4, 5, 6
 ),

  final AS ( 

    select
      fa.member_id,
      fa.manager_id,
      fa.track_assignment_id,
      eng.first_completed_session_at,
      datediff(days, eng.first_completed_session_at,
                 fa.feedback_request_submitted_at) days_from_first_session_to_member_request,
      fa.feedback_request_assessment_id,
      fa.feedback_request_submitted_at,
      case
            when fa.manager_feedback_assessment_submitted_at is null
                then datediff(days, fa.feedback_request_submitted_at, current_date())
            end as days_since_request_unanswered,
      case
            when fa.manager_feedback_assessment_submitted_at is not null
                then datediff(days, fa.feedback_request_submitted_at, fa.manager_feedback_assessment_submitted_at)
            end as days_since_request_answered,
      a.assessment_id as manager_feedback_assessment_id,
      fa.manager_feedback_assessment_submitted_at,
      datediff(days, eng.first_completed_session_at,
                 coalesce(fa.manager_feedback_assessment_submitted_at, current_date())) days_from_first_session_to_manager_submission
    from
        feedback_assessments fa
        left join track_assignments ta on fa.track_assignment_id = ta.track_assignment_id
        left join assessments a on
                    fa.member_id = a.user_id
                and fa.manager_id = a.creator_id
                and fa.manager_feedback_assessment_submitted_at = a.submitted_at
        left join dei_member_engagement_by_track eng on fa.member_id = eng.member_id
            and ta.track_id = eng.track_id
),

query as (
select 
  --primary key
    {{ dbt_utils.surrogate_key(['member_id', 'manager_id', 'track_assignment_id', 'feedback_request_assessment_id']) }} AS primary_key,
    
  --foreign keys
    member_id,
    manager_id,
    track_assignment_id,
    feedback_request_assessment_id,
    manager_feedback_assessment_id,
  
  --timestamps
    feedback_request_submitted_at,
    manager_feedback_assessment_submitted_at,
    first_completed_session_at,

  --measures
    days_from_first_session_to_member_request,
    days_from_first_session_to_manager_submission,
    days_since_request_unanswered,
    days_since_request_answered
from final 
)

select * from query