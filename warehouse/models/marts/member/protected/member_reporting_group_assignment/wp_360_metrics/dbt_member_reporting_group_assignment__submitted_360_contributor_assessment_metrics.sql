WITH events__submitted_assessment AS (

    SELECT
    *,
    attributes:"assessment_type" AS assessment_type
    FROM {{ref('dbt_events__submitted_assessment')}}

),

    reporting_group_assignments AS (

    SELECT * FROM {{ref('dim_reporting_group_assignments')}}

),

    submitted_360_assessment_metrics AS (

    SELECT * FROM {{ref('dbt_member_reporting_group_assignment__submitted_360_assessment_metrics')}}

    ),

wp_360_contributor_assessments AS (

    SELECT
        attributes:"parent_id" AS parent_id,
        SUM(CASE
            WHEN attributes:"responses_role" = 'manager' THEN 1
            ELSE 0 END) AS manager_response_count,
        SUM(CASE
            WHEN attributes:"responses_role" = 'other_coworker' THEN 1
            ELSE 0 END) AS other_coworker_response_count,
        SUM(CASE
            WHEN attributes:"responses_role" = 'direct_report' THEN 1
            ELSE 0 END) AS direct_report_response_count
    FROM events__submitted_assessment
    WHERE assessment_type = 'Assessments::WholePerson360ContributorAssessment'
    GROUP BY 1

),

final as (
        
        SELECT
            {{ dbt_utils.surrogate_key(['member_id', 'reporting_group_id', 'associated_assignment_id']) }} as member_reporting_group_assignment_key,
            member_id,
            reporting_group_id,
            associated_assignment_id,
            other_coworker_response_count,
            direct_report_response_count
        FROM wp_360_contributor_assessments as wp_360_contrib
            JOIN submitted_360_assessment_metrics as wp_360_req
        ON wp_360_contrib.parent_id = wp_360_req.assessment_id
        GROUP BY 1, 2, 3, 4, 5, 6

)

select * from final
