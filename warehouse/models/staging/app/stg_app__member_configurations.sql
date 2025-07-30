{{
  config(
    tags=["eu"]
  )
}}

WITH member_configurations AS (
  SELECT * FROM {{ source('app', 'member_configurations') }}
)

SELECT
    id AS member_configuration_id,
    default_resource_list_id,
    includes_behavioral_assessments,
    coalesce(whole_person360_enabled, false) AS whole_person360_enabled, -- sometimes NULL in app db, map NULL to FALSE here
    coalesce(whole_person180_enabled, false) AS whole_person180_enabled, -- sometimes NULL in app db, map NULL to FALSE here
    disable_confirmation_email,
    manager_feedback_enabled,
    manager_required_360

FROM member_configurations