with source as (

    select * from {{ source('gsheets_ops_analytics', 'cso_time_card') }}

),

renamed as (

    select
        {{ dbt_utils.surrogate_key(['zendesk_user_id__st', 'work_date']) }} as primary_key,
        zendesk_user_id__st,
        employee_number,
        first_name,
        last_name,
        name_cleaned,
        ot_hours,
        reg_hours,
        work_date,
        zendesk_user_id

    from source

)

select * from renamed

