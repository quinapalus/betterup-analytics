with source as (

    select * from {{ source('app', 'scheduled_invitations') }}

),

renamed as (

    select
        id as scheduled_invitation_id,
        invitation_id,
        invite_track_id,
        inviting_user_id,
        scheduled_job_id,
        invitation_type,
        invitee_email,
        language,
        options,
        product_subscription_data,
        send_date,
        {{ load_timestamp('created_at') }},
        {{ load_timestamp('updated_at') }}

    from source

)

select * from renamed