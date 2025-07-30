{{
  config(
    tags=["eu"]
  )
}}

WITH coach_configurations AS (

  SELECT * FROM {{ source('app', 'coach_configurations') }}

)

SELECT
    id AS coach_configuration_id,
    overview,
    program_briefing_duration_minutes,
    program_briefing_automatic_payment,
    success_criteria,
    external_resource_video_links
FROM coach_configurations