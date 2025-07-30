with source as (
  select * from {{ source('app', 'session_credits') }}
),

renamed as (
select 
    --primary key
    id as session_credit_id,
    
    --foreign keys
    appointment_id,
    creator_id as created_by_user_id,
    order_id,
    user_id,

    --attributes
    reason,
    iff(order_id is not null, true, false) as is_flex_session,
    iff(appointment_id is not null, true, false) as is_redeemed_session,

    --timestamps
    created_at,
    expired_at,
    expires_on,    
    voided_at

from source
)

select * from renamed