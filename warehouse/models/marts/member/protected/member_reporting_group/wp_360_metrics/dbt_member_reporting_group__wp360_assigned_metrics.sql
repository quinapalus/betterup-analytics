WITH activities AS (

    SELECT * FROM  {{ ref('stg_app__activities') }}

), resources AS (

    SELECT * FROM  {{ ref('stg_app__resources') }}

), wp_360_activities AS (
                 
    SELECT * FROM activities
    WHERE resource_id IN (
        SELECT  resource_id FROM resources
        WHERE content = 'Assessments::WholePerson360Assessment'
                )
                
), reporting_group_assignments AS (

    SELECT * FROM {{ref('dim_reporting_group_assignments')}}

)


SELECT
  {{ dbt_utils.surrogate_key(['rga.member_id', 'rga.reporting_group_id']) }} AS primary_key,
    rga.member_id,
    rga.reporting_group_id,
    max(wp360.created_at) as created_at
FROM reporting_group_assignments AS rga
INNER JOIN wp_360_activities AS wp360
                    ON rga.member_id = wp360.member_id AND
                       wp360.created_at >= rga.starts_at AND
                       (rga.ended_at IS NULL OR wp360.created_at < rga.ended_at)
GROUP BY rga.member_id, rga.reporting_group_id



