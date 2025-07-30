with construct_benchmark_scores_reference_population_1 as (

    select * from {{ ref('int_app__construct_benchmark_scores_reference_population_1') }}

),

construct_benchmark_scores_reference_population_2 as (

    select * from {{ ref('int_app__construct_benchmark_scores_reference_population_2') }}

),

construct_benchmark_scores_union as (
    select
        construct_type,
        reference_population_id,
        reference_population_key,
        benchmark_population_type,
        NULL as country,
        industry,
        level,
        key,
        mean
    from construct_benchmark_scores_reference_population_1

    union all

    select
        construct_type,
        reference_population_id,
        reference_population_key,
        benchmark_population_type,
        country,
        industry,
        level,
        key,
        mean
    from construct_benchmark_scores_reference_population_2
)

select
    {{ dbt_utils.surrogate_key(['reference_population_key', 'construct_type', 'country', 'industry', 'level', 'key']) }} as primary_key,
    *
from construct_benchmark_scores_union