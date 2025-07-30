WITH feedback_requests AS (

  SELECT * FROM {{ref('stg_app__feedback_requests')}}

),

feedback_contributors AS (

  SELECT * FROM {{ref('stg_app__feedback_contributors')}}

),

reporting_group_assignments AS (

  SELECT * FROM {{ref('dim_reporting_group_assignments')}}

),

feedback_events AS    (

    SELECT
        feedback_request.member_id,
        feedback_contributor.user_id AS contributor_id,
        feedback_request.created_at AS contributor_invited_at,
        feedback_request.report_available_at AS feedback_submitted_at
    FROM feedback_requests AS feedback_request

    INNER JOIN feedback_contributors AS feedback_contributor
        ON feedback_contributor.feedback_request_id = feedback_request.feedback_request_id
),

final AS (

    SELECT
        rga.member_id,
        fe.contributor_id,
        rga.reporting_group_id,
        MIN(fe.contributor_invited_at) contributor_invited_at,
        MIN(fe.feedback_submitted_at) first_feedback_submitted_at,
        MAX(fe.feedback_submitted_at) last_feedback_submitted_at
    FROM reporting_group_assignments AS rga
    LEFT JOIN feedback_events as fe
        ON fe.member_id = rga.member_id AND
           fe.contributor_invited_at >= rga.starts_at AND
           (rga.ended_at IS NULL OR fe.contributor_invited_at < rga.ended_at)
    GROUP BY
        rga.member_id,
        fe.contributor_id,
        rga.reporting_group_id

)

select
    {{ dbt_utils.surrogate_key(['member_id', 'contributor_id', 'reporting_group_id']) }} as member_reporting_group_contributor_key,
    *
from final
