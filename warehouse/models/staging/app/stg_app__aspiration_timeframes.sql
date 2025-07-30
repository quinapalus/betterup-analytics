with source as (

    select * from {{ source('app', 'aspiration_timeframes') }}

),

renamed as (

    select
        id as aspiration_timeframe_id,
        description,
        translation_key,
        {{ load_timestamp('created_at') }},
        {{ load_timestamp('updated_at') }}

    from source

)

select * from renamed