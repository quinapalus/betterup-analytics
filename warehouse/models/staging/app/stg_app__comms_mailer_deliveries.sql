with source as (

    select * from {{ source('app', 'comms_mailer_deliveries') }}

),

renamed as (

    select
        id as comms_mailer_delivery_id,
        mailer,
        mailer_action,
        recipients,
        tracking_uuid,
        template_digest,
        template_file,
        subject,
        {{ load_timestamp('created_at') }},
        {{ load_timestamp('updated_at') }}


    from source

)

select * from renamed