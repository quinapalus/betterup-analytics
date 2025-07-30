with billable_events as (
    select * from {{ ref('stg_app__billable_events')}}
),

product_subscription_assignments as (
    select * from {{ ref('stg_app__product_subscription_assignments')}}
),

joined as (
select 
    billable_events.*,
    product_subscription_assignments.stripe_subscription_id
from billable_events
left join product_subscription_assignments
    on billable_events.product_subscription_assignment_id = product_subscription_assignments.product_subscription_assignment_id
),

final as (
    select
        --primary key
        billable_event_id,

        --foreign keys
        stripe_subscription_id,
        product_subscription_assignment_id,
        coach_id,
        member_id,
        track_id,

        --attributes
        event_type,

        --timestamps
        event_at,

        --measures
        amount_due_usd
    from joined
)

select * from final