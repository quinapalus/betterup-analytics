-- This is unioning together seed files
-- This has more logic than a staging file should have,
-- but storing in staging to make more consistent w ref_pop 1 structure.
with construct_benchmark_scores_reference_population_2_country as (

    select * from {{ ref('construct_benchmark_scores_reference_population_2_country') }}

),

construct_benchmark_scores_reference_population_2_industry as (

    select * from {{ ref('construct_benchmark_scores_reference_population_2_industry') }}

),

construct_benchmark_scores_reference_population_2_level as (

    select * from {{ ref('construct_benchmark_scores_reference_population_2_level') }}

),

construct_benchmark_scores_reference_population_2_industry_level as (

    select * from {{ ref('construct_benchmark_scores_reference_population_2_industry_level') }}

),

reference_population_2_union as (
    select
        reference_population_id,
        reference_population_key,
        benchmark_type as benchmark_population_type,
        country,
        NULL as industry,
        NULL as level,
        key,
        mean
    from construct_benchmark_scores_reference_population_2_country

    union all

    select
        reference_population_id,
        reference_population_key,
        benchmark_type as benchmark_population_type,
        NULL as country,
        industry,
        NULL as level,
        key,
        mean
    from construct_benchmark_scores_reference_population_2_industry

    union all

    select
        reference_population_id,
        reference_population_key,
        benchmark_type as benchmark_population_type,
        NULL as country,
        NULL as industry,
        level,
        key,
        mean
    from construct_benchmark_scores_reference_population_2_level

    union all

    select
        reference_population_id,
        reference_population_key,
        benchmark_type,
        NULL as country,
        industry,
        level,
        key,
        mean
    from construct_benchmark_scores_reference_population_2_industry_level
)

select
    {{ dbt_utils.surrogate_key(['country', 'industry', 'level', 'key']) }} as primary_key,
    *
from reference_population_2_union