WITH primary_coach_assignments AS (

  SELECT * FROM {{ref('dim_coach_assignments')}}
  WHERE role = 'primary'

),

days as (
    
    select * from {{datespine("day","'2015-02-09'::timestamp","current_date")}}
    
)


SELECT
  pca.member_id,
  pca.coach_id AS primary_coach_id,
  days.date_day
FROM primary_coach_assignments as pca
join days
    on date_trunc('day', pca.created_at) <= days.date_day
    -- Greater than end date of the generated set so that member is not considered matched on the day the coach assignment ended.
    and coalesce(date_trunc('day', pca.ended_at), current_date) > days.date_day
