WITH events__submitted_assessment AS (

  SELECT
    *,
    attributes:"assessment_type" AS assessment_type
  FROM {{ref('dbt_events__submitted_assessment')}}

),

reporting_group_assignments AS (

  SELECT * FROM {{ref('dim_reporting_group_assignments')}}

),

reflection_point_assessments AS (

  SELECT * FROM events__submitted_assessment
  WHERE assessment_type = 'Assessments::WholePersonProgramCheckinAssessment'

),

final as (
    SELECT
      rga.member_id,
      rga.reporting_group_id,
      MAX(rp.event_at) AS submitted_last_reflection_point_at,
      COUNT(rp.associated_record_id) AS count_completed_reflection_points
    FROM reporting_group_assignments AS rga
    INNER JOIN reflection_point_assessments as rp
        ON rga.member_id = rp.member_id AND
           rp.event_at >= rga.starts_at AND
           (rga.ended_at IS NULL OR rp.event_at < rga.ended_at)
    GROUP BY rga.member_id, rga.reporting_group_id
)

select
    {{ dbt_utils.surrogate_key(['member_id', 'reporting_group_id']) }} AS primary_key,
    *
from final
