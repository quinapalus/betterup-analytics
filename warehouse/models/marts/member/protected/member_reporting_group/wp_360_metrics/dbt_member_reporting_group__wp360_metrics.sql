WITH reporting_group_assignments AS (

    SELECT member_id, reporting_group_id FROM {{ref('dim_reporting_group_assignments')}}
    GROUP BY 1, 2

),

    wp360_assigned_metrics AS (

SELECT * FROM {{ref('dbt_member_reporting_group__wp360_assigned_metrics')}}

    ),


    started_360_assessment_metrics AS (

SELECT * FROM {{ref('dbt_member_reporting_group__started_360_assessment_metrics')}}

    ),

    submitted_360_assessment_metrics AS (

SELECT * FROM {{ref('dbt_member_reporting_group__submitted_360_assessment_metrics')}}

    ),

    submitted_360_contributor_assessment_metrics AS (

SELECT * FROM {{ref('dbt_member_reporting_group__submitted_360_contributor_assessment_metrics')}}

    )

SELECT
    {{ dbt_utils.surrogate_key(['rga.member_id', 'rga.reporting_group_id']) }} AS primary_key,
    rga.member_id,
    rga.reporting_group_id,
    CASE
    WHEN (wp360_submitted_contrib.other_coworker_response_count
    + wp360_submitted_contrib.direct_report_response_count)  > 1 THEN 'Completed'
    WHEN wp360_submitted.submitted_last_360_assessment_at IS NOT NULL THEN 'Pending Contributor Action'
    WHEN wp360_started.started_last_360_assessment_at IS NOT NULL
             AND DATEDIFF(WEEK, started_last_360_assessment_at, CURRENT_TIMESTAMP) > 1 THEN 'Pending Member Action'
    WHEN wp360_started.started_last_360_assessment_at IS NOT NULL THEN 'Started'
    END AS wp_360_status

FROM reporting_group_assignments AS rga
LEFT OUTER JOIN wp360_assigned_metrics AS wp360_assigned
    ON rga.member_id = wp360_assigned.member_id AND
        rga.reporting_group_id = wp360_assigned.reporting_group_id
LEFT OUTER JOIN started_360_assessment_metrics AS wp360_started
    ON rga.member_id = wp360_started.member_id AND
        rga.reporting_group_id = wp360_started.reporting_group_id
LEFT OUTER JOIN submitted_360_assessment_metrics AS wp360_submitted
    ON rga.member_id = wp360_submitted.member_id AND
        rga.reporting_group_id = wp360_submitted.reporting_group_id
LEFT OUTER JOIN submitted_360_contributor_assessment_metrics AS wp360_submitted_contrib
    ON rga.member_id = wp360_submitted_contrib.member_id AND
        rga.reporting_group_id = wp360_submitted_contrib.reporting_group_id
