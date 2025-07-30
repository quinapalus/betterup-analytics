WITH manager_feedback_flows AS (

  SELECT * FROM {{ref('stg_app__manager_feedback_flows')}}

),

 assessments AS (

  SELECT * FROM {{ref('stg_app__assessments')}}

),

reporting_group_assignments AS (

  SELECT * FROM {{ref('dim_reporting_group_assignments')}}

),

assessment_events AS    (

    SELECT
        ff.member_id,
        ff.manager_id,
        IFF(ff.opted_out_at IS NULL, ff.created_at, NULL) AS manager_invited_at,
        feed_ass.submitted_at AS manager_feedback_submitted_at,
        growth_ass.submitted_at AS manager_growth_assessment_submitted_at
    FROM manager_feedback_flows AS ff

    LEFT JOIN assessments AS feed_ass
        ON ff.manager_id = feed_ass.creator_id AND
           ff.member_id = feed_ass.user_id AND
           ff.manager_feedback_assessment_id = feed_ass.assessment_id AND
           feed_ass.type = 'Assessments::ManagerFeedbackAssessment'

    LEFT JOIN assessments AS growth_ass
        ON ff.manager_id = growth_ass.creator_id AND
           ff.member_id = growth_ass.user_id AND
           ff.manager_growth_assessment_id = growth_ass.assessment_id AND
           growth_ass.type = 'Assessments::ManagerGrowthAssessment'
),

final as (
    SELECT
        {{ dbt_utils.surrogate_key(['rga.member_id', 'rga.reporting_group_id', 'rga.associated_assignment_id', 'ae.manager_id']) }} as member_reporting_group_assignment_manager_key,
        rga.member_id,
        ae.manager_id,
        rga.reporting_group_id,
        rga.associated_assignment_id,
        MIN(ae.manager_invited_at) manager_invited_at,
        MIN(ae.manager_feedback_submitted_at) manager_feedback_submitted_at,
        MIN(ae.manager_growth_assessment_submitted_at) manager_growth_assessment_submitted_at
    FROM reporting_group_assignments AS rga
    LEFT JOIN assessment_events as ae
         ON ae.member_id = rga.member_id AND
            ae.manager_invited_at >= rga.starts_at AND
            (rga.ended_at IS NULL OR ae.manager_invited_at < rga.ended_at)
    GROUP BY
        member_reporting_group_assignment_manager_key,
        rga.member_id,
        ae.manager_id,
        rga.associated_assignment_id,
        rga.reporting_group_id
)

select * from final
