{{
  config(
    tags=['classification.c3_confidential']
  )
}}

SELECT
  *
FROM {{ref('stg_app__timeslots')}}
-- Only look at the coach availability that is under 24 hours. This ensures that
-- we are not crossing over multiple days in UTC. Product has been notified of this
-- in January 2020 in #squad-internal-tools.
WHERE DATEDIFF('HOUR', starts_at, ends_at) < 24
