with event_subscription_created as(
  select * from {{ source('segment_backend', 'subscription_created') }}
),
renamed as (
  select 
       id
      ,has_primary_coach
      ,original_timestamp
      ,received_at
      ,user_id
      ,context_library_name
      ,context_library_version
      ,event_text
      ,deployment_type
      ,event
      ,sent_at
      ,track_id
      ,uuid_ts
      ,timestamp
      ,track_assignment_id
      ,consumer_product_id
      ,consumer_product_price_nickname
      ,consumer_product_price_stripe_price_id
      ,consumer_product_price_unit_amount
      ,consumer_product_stripe_product_id
      ,consumer_product_price_id
      ,consumer_product_name
      ,consumer_product_price_recurring_interval
      ,consumer_product_price_recurring_interval_count
      ,consumer_product_price_currency
      ,consumer_product_sessions_per_month
      ,consumer_subscription_trial_ends_on
      ,consumer_subscription_trial
      ,upfront_subscription_state
      ,hubspot_contact_id
      ,product_subscription_assignment_id
      ,product_subscription_id
      ,product_id
      ,browser_name
      ,browser_version
      ,platform_version
      ,device_name
      ,platform_name
      ,app_version
      ,app_build
from event_subscription_created
)

select * from renamed
