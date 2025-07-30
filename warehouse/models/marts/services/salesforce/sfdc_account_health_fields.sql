{{
  config(
    tags=['classification.c3_confidential']
  )
}}

WITH account_health_current_snapshot AS (
  SELECT
    *
  FROM {{ ref('dim_accounts_daily_snapshot') }}
  WHERE
    has_ever_been_eligible_for_account_health_scoring
    AND is_currently_valid
)

SELECT * FROM account_health_current_snapshot
