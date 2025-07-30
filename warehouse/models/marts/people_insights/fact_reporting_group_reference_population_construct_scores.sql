with reference_population_construct_scores as (

    select * from {{ ref('fact_reference_population_construct_scores') }}

),

reporting_group_assignments as (

    select * from {{ ref('dim_reporting_group_assignments') }}
    
)

-- For purposes of this fact table, we just want to join through reporting_group_assignments
--   to link a member to the reporting group. This dedup avoids fanout from one member having
--   multiple reporting group assignments to the same reporting group
, reporting_group_assignments_grouped as (

    select
        {{ dbt_utils.surrogate_key(['member_id', 'reporting_group_id', 'starts_at']) }} as member_reporting_group_assignment_key,
        member_id,
        reporting_group_id,
        reporting_group_name,
        starts_at,
        nullif(max(coalesce(ended_at,'9999-12-31')),'9999-12-31') as ended_at
    from reporting_group_assignments
    group by 1,2,3,4,5

)

select
    {{ dbt_utils.surrogate_key(['s.primary_key', 'rga.member_reporting_group_assignment_key']) }} as primary_key,
    rga.reporting_group_id,
    rga.reporting_group_name,
    {{ dbt_utils.star(from=ref('fact_reference_population_construct_scores'), except=["primary_key"], relation_alias='s') }}
from reference_population_construct_scores as s
inner join reporting_group_assignments_grouped as rga
    on s.member_id = rga.member_id
        and s.submitted_at >= rga.starts_at 
        and (rga.ended_at is null or s.submitted_at < rga.ended_at)
