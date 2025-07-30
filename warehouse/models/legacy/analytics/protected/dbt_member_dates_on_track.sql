WITH track_assignments AS (

  SELECT * FROM {{ref('dbt_track_assignments')}}

),

days as (
    
    select * from {{datespine("day","'2015-02-09'::timestamp","current_date")}}
    
)


SELECT
  ta.member_id,
  ta.track_id,
  ta.track_assignment_id,
  days.date_day
FROM track_assignments AS ta
join days
    on date_trunc('day', ta.created_at) <= days.date_day
    -- Greater than end date of the generated set so that member is not considered active on the day they were closed out.
    and coalesce(date_trunc('day', ta.ended_at), current_date) > days.date_day
