{{ config(
    tags=["identify_ai_metrics"],
) }}


with reporting_group_assignments as (

  select member_id, reporting_group_id 
  from {{ref('dim_reporting_group_assignments')}}
  where associated_record_type = 'Track'
  group by member_id, reporting_group_id

),

reporting_group_engagement_metrics as (

  select member_id, reporting_group_id, ended_at from {{ref('fact_reporting_group_engagement_metrics')}}
  where reporting_group_id in (
        select reporting_group_id from  {{ref('dim_reporting_groups')}}
        where product_type in ( 'growth_and_transformation',
                                'sales_performance',
                                'diversity_equity_inclusion_and_belonging',
                                'primary_coaching'
         )
    )
  -- member has started coaching
  and status = 'started_coaching'
  -- access ending within the next 30 days
  and ended_at between getdate() and dateadd(day,30,getdate())

),

submitted_rp_assessment_metrics as (

  select member_id, reporting_group_id, count_completed_reflection_points 
  from {{ref('dbt_member_reporting_group__submitted_reflection_point_assessment_metrics')}}
  group by member_id, reporting_group_id, count_completed_reflection_points 

)

select
  {{ dbt_utils.surrogate_key(['rgem.member_id', 'rgem.reporting_group_id']) }} as member_reporting_group_key,
  rgem.member_id, 
  rgem.reporting_group_id, 
  rgem.ended_at, 
  rp.count_completed_reflection_points
from reporting_group_engagement_metrics as rgem
inner join reporting_group_assignments as rga
  on rga.member_id = rgem.member_id and 
    rga.reporting_group_id = rgem.reporting_group_id 
left outer join submitted_rp_assessment_metrics as rp
  on rgem.member_id = rp.member_id and 
    rgem.reporting_group_id = rp.reporting_group_id
-- For members who have completed at least 1 Reflection Point assessment
-- WHERE rp.count_completed_reflection_points >= 1