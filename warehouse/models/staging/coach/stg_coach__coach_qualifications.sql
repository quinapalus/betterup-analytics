with source as (

    select * from {{ source('coach', 'coach_qualifications') }}

),

renamed as (

    select
        id as coach_qualification_id,
        {{ load_timestamp('created_at') }},
        {{ load_timestamp('updated_at') }},
        category,
        name

    from source

)

select * from renamed
