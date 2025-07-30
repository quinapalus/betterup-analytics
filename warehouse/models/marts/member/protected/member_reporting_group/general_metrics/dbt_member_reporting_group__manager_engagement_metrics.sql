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

final AS (

    SELECT
        rga.member_id,
        ae.manager_id,
        rga.reporting_group_id,
        MIN(ae.manager_invited_at) manager_invited_at,
        MIN(ae.manager_feedback_submitted_at) manager_feedback_submitted_at,
        MIN(ae.manager_growth_assessment_submitted_at) manager_growth_assessment_submitted_at
    FROM reporting_group_assignments AS rga
    LEFT JOIN assessment_events as ae
         ON ae.member_id = rga.member_id AND
            ae.manager_invited_at >= rga.starts_at AND
            (rga.ended_at IS NULL OR ae.manager_invited_at < rga.ended_at)
    GROUP BY
        rga.member_id,
        ae.manager_id,
        rga.reporting_group_id

)

select
    {{ dbt_utils.surrogate_key(['member_id', 'manager_id', 'reporting_group_id']) }} as member_reporting_group_manager_key,
    *
from final
