with source as (

    select * from {{ source('app', 'generated_messages') }}

),

renamed as (

    select
        -- ids
        id as generated_message_id,
        appointment_id,
        coach_id,
        user_id,

        --dates
        created_at,
        updated_at,

        --misc
        message_type,
        request,
        response,
        status

    from source

)

select * from renamed