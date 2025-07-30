WITH open_reporting_group_assignments AS (

  SELECT member_id, reporting_group_id, associated_assignment_id FROM {{ref('dim_reporting_group_assignments')}}
  WHERE ended_at IS NULL
  GROUP BY 1, 2, 3

),

current_reflection_points AS (

  SELECT * FROM {{ref('dbt_app__reflection_points')}}
  -- filter for most recent reflection point for each member where
  -- prerequisites have been met
  WHERE met_prerequisites_at IS NOT NULL
  QUALIFY ROW_NUMBER() OVER (PARTITION BY member_id ORDER BY met_prerequisites_at DESC) = 1

),

eligible_reflection_points AS (

  SELECT
    *
  FROM current_reflection_points
  -- within current reflection point, filter for records that aren't canceled
  -- and where the member has not submitted their assessment
  WHERE canceled_at IS NULL
    AND member_assessment_submitted_at IS NULL

),

final as (
 
  SELECT
    {{ dbt_utils.surrogate_key(['rga.member_id', 'rga.reporting_group_id', 'rga.associated_assignment_id']) }} as member_reporting_group_assignment_key,    
    rga.member_id,
    rga.reporting_group_id,
    rga.associated_assignment_id,
    CASE
      -- if member is on open reporting group assignment and no eligible
      -- reflection point record, status is not eligible
      WHEN rp.reflection_point_id IS NULL THEN 'not eligible'
      -- if coach complete or 7 days after coach due, status is pending
      -- member completion
      WHEN rp.coach_assessment_submitted_at IS NOT NULL
        OR CURRENT_TIMESTAMP > DATEADD(DAY, 7, rp.coach_due_at)
        THEN 'pending member completion'
      WHEN CURRENT_TIMESTAMP > rp.eligible_at THEN 'pending coach completion'
      ELSE 'upcoming - process starting ' || TO_CHAR(rp.eligible_at, 'YYYY-MM-DD')
    END AS current_reflection_point_status
  FROM open_reporting_group_assignments AS rga
  LEFT OUTER JOIN eligible_reflection_points AS rp
    ON rga.member_id = rp.member_id

)

select * from final 
