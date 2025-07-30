with source as (

    select * from {{ source('salesforce', 'history_my_forecast') }}

),

renamed as (

    select
        id as history_my_forecast_id,
        new_value,
        old_value,
        parent_id,
        received_at,
        uuid_ts,
        created_date,
        is_deleted,
        created_by_id,
        field

    from source

)

select * from renamed