{{
  config(
    tags=["eu"]
  )
}}

WITH admin_configurations AS (

  SELECT * FROM {{ source('app', 'admin_configurations') }}

)

SELECT id AS admin_configuration_id,
    customer_goals,
    deployment_type,
    deployment_cadence,
    internal_notes,
    key_satisfaction_driver,
    {{ load_timestamp('launches_on') }},
    nullif(member_orientation, '') as member_orientation,
    wpm_behavior_goals
FROM admin_configurations