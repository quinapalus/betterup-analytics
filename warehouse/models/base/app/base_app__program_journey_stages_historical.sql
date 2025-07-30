with archived_program_journey_stages as (

    select * from {{ source('app_archive', 'program_journey_stages') }}

)

select * from archived_program_journey_stages