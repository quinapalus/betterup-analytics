{{
  config(
    tags=['eu']
  )
}}

with assessments as (
    
    select * from {{ ref('int_app__assessments') }}

),

assessment_contributors as (

    select * from {{ ref('stg_app__assessment_contributors') }}

)

select
    ac.assessment_contributor_id,
    ac.assessment_id as parent_assessment_id,
    ac.role,
    ac.contributor_id,
    coalesce(ac.response_assessment_id, unsubmitted_response.assessment_id) as response_assessment_id,
    ac.created_at,
    coalesce(response.created_at, unsubmitted_response.created_at) as response_created_at,
    response.submitted_at as response_submitted_at,
    parent.report_generated_at,
    parent.user_id as parent_member_id,
    parent.created_at as parent_created_at,
    parent.submitted_at as parent_submitted_at,
    parent.track_assignment_id as parent_assessment_track_assignment_id,

    --flags
    iff(response.submitted_at is not null, true,false) as is_response_submitted,
    iff(parent.report_generated_at is not null,true,false) as is_report_generated,
    iff(response.submitted_at < parent.report_generated_at,true,false) as is_response_included_in_report

from assessment_contributors as ac
inner join assessments as parent
    on ac.assessment_id = parent.assessment_id
left join assessments as response
    on ac.response_assessment_id = response.assessment_id
left join assessments as unsubmitted_response
    -- limit join to unsubmitted responses to cases where we haven't already joined in a submitted response
    on response.assessment_id is null and
        ac.assessment_id = unsubmitted_response.parent_id and
        ac.contributor_id = unsubmitted_response.creator_id and
        unsubmitted_response.submitted_at is null 
