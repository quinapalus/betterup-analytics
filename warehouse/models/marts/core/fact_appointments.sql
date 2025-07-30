WITH appointments AS (
  -- pass through staging model
  SELECT * FROM {{ ref('stg_app__appointments') }}
)

SELECT * FROM appointments
