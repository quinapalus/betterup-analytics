WITH event_user_confirmed AS(
  SELECT *, row_number() over(PARTITION BY user_id ORDER BY confirmed_at) as rn
  FROM {{ source('segment_backend', 'user_confirmed') }}
)

SELECT *
FROM event_user_confirmed
WHERE rn = 1
