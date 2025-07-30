with source as (

    select * from {{ source('coach', 'coach_profile_solutions') }}

),

renamed as (

    select
        id as coach_profile_solution_id,
        uuid as coach_profile_solution_uuid,
        {{ load_timestamp('created_at') }},
        {{ load_timestamp('updated_at') }},
        coach_profile_uuid,
--        solution_id,  -- only usable US data
        solution_uuid

    from source

)

select * from renamed
