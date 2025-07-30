with source as (

    select * from {{ source('app', 'mentor_assignments') }}

),

renamed as (

    select
        id as mentor_assignment_id,
        mentee_id,
        mentor_id,
        mentee_program_assignment_id,
        mentor_program_assignment_id,
        {{ load_timestamp('ended_at') }},
        {{ load_timestamp('created_at') }},
        {{ load_timestamp('updated_at') }}

    from source

)

select * from renamed