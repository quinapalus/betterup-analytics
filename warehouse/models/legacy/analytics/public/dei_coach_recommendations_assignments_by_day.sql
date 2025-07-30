with coach_date as (
  select * from {{ref('dbt_coach_date')}}
),
coach_date_recommendations as (
  select * from {{ref('dbt_coach_date_recommendations')}}
),
coach_date_assignments as (
  select * from {{ref('dbt_coach_date_assignments')}}
),
final as (
  select
    {{ dbt_utils.surrogate_key(['cd.coach_id','cd.day'])}} as coach_recommendations_assignments_by_day_id,
    cd.coach_id,
    cd.day as date_day,
    COALESCE(cr.recommendation_count, 0) as recommendation_count,
    COALESCE(ca.assignment_count, 0) as assignment_count
  from coach_date as cd
  left join coach_date_recommendations as cr
    on cd.coach_id = cr.coach_id
    and cd.day = cr.date_day
  left join coach_date_assignments as ca
    on cd.coach_id = ca.coach_id
    and cd.day = ca.date_day
)
select *
from final
