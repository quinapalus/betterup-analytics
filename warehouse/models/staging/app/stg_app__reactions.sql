with source as (

    select * from {{ source('app', 'reactions') }}

),

renamed as (

    select
        id as reaction_id,
        reactable_id,
        category,
        count,
        reactable_type,
        {{ load_timestamp('created_at') }},
        {{ load_timestamp('updated_at') }}

    from source

)

select * from renamed