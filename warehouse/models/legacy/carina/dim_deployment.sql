{{
  config(
    tags=['classification.c3_confidential','eu']
  )
}}

WITH tracks AS (

  SELECT * FROM {{ref('dim_tracks')}}
  WHERE is_external and engaged_member_count is not null --this logic was in dei_tracks which this model used to reference

)

SELECT
  {{ deployment_key('track_id') }} AS deployment_key,
  track_id AS app_track_id,
  name as track_name,
  CASE
    WHEN program_name IS NOT NULL AND program_name <> ''
    THEN organization_name || ' - ' || program_name
    ELSE 'N/A'
  END AS program_name,
  start_date,
  end_date,
  is_active AS deployment_is_currently_active,
  open_track_assignment_count AS open_member_count,
  engaged_member_count,
  is_revenue_generating,
  deployment_type,
  sfdc_prospective_opportunity_id,
  member_orientation,
  accounting_category AS parent_accounting_category,
  admin_panel_url,
  partner_panel_url
FROM tracks
