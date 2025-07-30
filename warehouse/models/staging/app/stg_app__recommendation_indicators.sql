with source as (
    select * from {{ source('app', 'recommendation_indicators') }}
),
renamed as (

    select
        id AS recommendation_indicator_id,
        {{ load_timestamp('created_at') }},
        {{ load_timestamp('updated_at') }},
        indicator_name,
        weight

    from source

)

select * from renamed
