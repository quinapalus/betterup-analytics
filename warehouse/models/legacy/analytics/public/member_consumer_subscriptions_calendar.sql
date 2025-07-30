{{
  config(
    tags=['classification.c3_confidential','eu'],
    materialized='view'
  )
}}

WITH dim_date AS (
  SELECT * FROM {{ref('dim_date')}}
),

consumer_subscriptions AS (
  SELECT * FROM  {{ref('stg_app__consumer_subscriptions')}}
),

final AS (
  SELECT
    c.date_key,
    c.date,
    CASE
      WHEN
        s.trial_ends_on IS NULL THEN TRUE
      WHEN
        COALESCE(DATE_TRUNC('DAY', s.trial_ended_at), DATE_TRUNC('DAY', s.trial_ends_on)) <= c.date AND
        (DATE_TRUNC('DAY', s.ended_at) != DATE_TRUNC('DAY', s.trial_ended_at) OR s.ended_at IS NULL) THEN TRUE -- Logic to exclude trial only members
      ELSE FALSE
    END AS is_paid_subscription,
    c.calendar_year_month,
    c.is_current_fiscal_quarter,
    c.is_previous_fiscal_quarter,
    c.date = LAST_DAY(c.date) AS is_last_day_of_month,
    s.app_subscription_id as subscription_id,
    s.track_assignment_id,
    ROW_NUMBER() OVER(PARTITION BY c.date_key, s.track_assignment_id ORDER BY COALESCE(s.ended_at, s.trial_ended_at) DESC) AS rn
  FROM dim_date AS c
  INNER JOIN consumer_subscriptions AS s
    ON c.date >= DATE_TRUNC('day', s.created_at) AND
       c.date < COALESCE(s.ended_at, DATEADD(DAY, 365, CURRENT_DATE()))
)

SELECT
   {{ dbt_utils.surrogate_key(['date_key', 'subscription_id', 'track_assignment_id', 'is_paid_subscription']) }} as unique_id,
   date_key,
   date,
   calendar_year_month,
   is_current_fiscal_quarter,
   is_previous_fiscal_quarter,
   is_last_day_of_month,
   subscription_id,
   track_assignment_id,
   is_paid_subscription
FROM final
WHERE rn = 1 -- Logic to select the subscription with the latest end date in cases where there are overlapping subscriptions for the same track assignment and time period
QUALIFY ROW_NUMBER() OVER (PARTITION BY date, subscription_id ORDER BY date) = 1 -- Logic that limits to unique record per subscription per day