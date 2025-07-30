 
 with assessments as (
    select * from {{ ref('stg_app__assessments') }}
),
coach_feedback as (
    select
      a.assessment_id,
      r.path as item_key,
      r.value::STRING as item_response
    from assessments as a
    join LATERAL FLATTEN (input => a.responses) as r
    where item_key = 'feedback'
),
final as (
    select assessment_id,
        item_response
    from coach_feedback
)
select * from final
