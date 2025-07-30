with source as (

    select * from {{ ref('bu_whole_person_benchmarks') }}

),

renamed as (

    select
        whole_person_model_version,
        construct_key as key,
        industry,
        employee_level as level,
        scale_score_mean as mean
    from source

)

select
    {{ dbt_utils.surrogate_key(['key','industry','level']) }} as primary_key,
    *
from renamed