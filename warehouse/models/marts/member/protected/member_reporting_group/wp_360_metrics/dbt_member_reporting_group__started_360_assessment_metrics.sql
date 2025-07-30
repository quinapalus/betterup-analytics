WITH events__started_assessment AS (

    SELECT
        *,
        attributes:"assessment_type" AS assessment_type
    FROM {{ref('dbt_events__started_assessment')}}
    WHERE attributes:"submitted_at" IS NOT NULL

),

reporting_group_assignments AS (

SELECT * FROM {{ref('dim_reporting_group_assignments')}}

    ),

    wp_360_assessments AS (

SELECT * FROM events__started_assessment
WHERE assessment_type = 'Assessments::WholePerson360Assessment'

    )


SELECT
  {{ dbt_utils.surrogate_key(['rga.member_id', 'rga.reporting_group_id']) }} AS primary_key,
    rga.member_id,
    rga.reporting_group_id,
    MAX(wp360.event_at) AS started_last_360_assessment_at
FROM reporting_group_assignments AS rga
         INNER JOIN wp_360_assessments as wp360
                    ON rga.member_id = wp360.member_id AND
                       wp360.event_at >= rga.starts_at AND
                       (rga.ended_at IS NULL OR wp360.event_at < rga.ended_at)
GROUP BY rga.member_id, rga.reporting_group_id