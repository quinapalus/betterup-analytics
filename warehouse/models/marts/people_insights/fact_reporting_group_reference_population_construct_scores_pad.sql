WITH reporting_group_reference_population_construct_scores AS (

  SELECT * FROM {{ref('fact_reporting_group_reference_population_construct_scores')}}

),

pad_filters as (
    select
        *
    from reporting_group_reference_population_construct_scores
    where construct_type in ('subdimension', 'dimension', 'construct')
)

select * from pad_filters