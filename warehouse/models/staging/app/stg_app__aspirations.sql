with source as (

    select * from {{ source('app', 'aspirations') }}

),

renamed as (

    select
        id as aspiration_id,
        user_id as member_id,
        aspiration_timeframe_id,
        growth_focus_area_selection_id,
        description,
        {{ load_timestamp('created_at') }},
        {{ load_timestamp('updated_at') }}

    from source

)

select * from renamed