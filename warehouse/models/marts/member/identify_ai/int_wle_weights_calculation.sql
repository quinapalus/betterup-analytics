{{ config(
    tags=["identify_ai_metrics"],
) }}

with member_total_sessions as (
select member_id, count(session_id) as total_sessions
from {{ref('sessions')}}
where completed_session_billable_event = true
group by member_id
),

member_events as (
SELECT
    members.member_id  AS member_id,
    hi_assessment_text_scores.work_life_event  AS wle
    FROM {{ref('fact_assessments')}}  AS member_assessments
INNER JOIN {{ref('fact_assessment_item_responses')}}  AS assessment_item_responses ON (member_assessments.assessment_id) = (assessment_item_responses.assessment_id)
INNER JOIN {{ref('dim_members')}}
  AS members ON (member_assessments.user_id) = (members.member_id)
INNER JOIN  {{ref('assessments__assessment_text_scores')}}  AS hi_assessment_text_scores ON assessment_item_responses.assessment_id = hi_assessment_text_scores.assessment_id
  AND assessment_item_responses.item_key = hi_assessment_text_scores.item_key
where (hi_assessment_text_scores.item_key ) IN ('additional_descriptive_sentences', 'additional_topics') and wle is not null
group by member_id, wle
)


select member_events.wle, avg(member_total_sessions.total_sessions) as avg_event_sessions
from member_total_sessions
inner join member_events on member_total_sessions.member_id = member_events.member_id
group by wle
