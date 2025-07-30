{{
  config(
    tags=["eu"]
  )
}}

with coach_profile_solutions as (

  select * from {{ ref('stg_coach__coach_profile_solutions') }}

),

solutions as (

  select * from {{ ref('stg_curriculum__solutions') }}

),

-- generate array list of solutions per coach_profile_id (1:1 with coach)
coach_profile_rollup as (
    select
        coach_profile_uuid,
        array_agg(distinct s.key) as solution_keys_array
    from coach_profile_solutions cps
    left join solutions s
        on cps.solution_uuid = s.solution_uuid
    group by 1
),

-- check solutions_array for coach having box checked in profile for these solutions
solution_flags as (
    select
        *,
        array_contains('growth_and_transformation'::variant, solution_keys_array) as has_solution_growth_and_transformation,
        array_contains('sales_performance'::variant, solution_keys_array) as has_solution_sales_performance,
        array_contains('diversity_equity_inclusion_and_belonging'::variant, solution_keys_array) as has_solution_diversity_equity_inclusion_and_belonging
    from coach_profile_rollup
)

select * from solution_flags
