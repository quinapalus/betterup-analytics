--TODO: Refactor this logic into dim_members and update downstream Looker explores

with int_user as (
    select * from {{ ref('int_app__users')}}
),

stripe_customers as (
    select * from {{ref('stg_stripe__customers')}}
),

user_attribution_log as (
    select * from {{ref('int_user_attribution_log')}}
),

user_first_touch_attribution as (
    select
        user_id,
        utm_source as first_touch_utm_source,
        utm_campaign as first_touch_utm_campaign,
        utm_medium as first_touch_utm_medium,
        utm_content as first_touch_utm_content,
        channel_attribution as first_touch_channel_attribution
    from user_attribution_log
    where attribution_order = 1
),

user_last_touch_attribution as (
    select
        user_id,
        utm_source as last_touch_utm_source,
        utm_campaign as last_touch_utm_campaign,
        utm_medium as last_touch_utm_medium,
        utm_content as last_touch_utm_content,
        channel_attribution as last_touch_channel_attribution
    from user_attribution_log
    where reverse_attribution_order = 1
),

joined_stripe as (
    select
        int_user.*,
        stripe_customers.stripe_customer_id,
        --TODO: There are track_id associated to stripe subscriptions that are not considered D2C.
        --Track_id = 4789, 4790 need to be evaluated. 
        --There are certain BetterUp User_ids with multiple stripe_customer_ids.
        iff(stripe_customers.stripe_customer_id is not null, true, false) as is_direct_to_consumer_customer
    from int_user
    left join stripe_customers
        on int_user.app_user_email_sk = stripe_customers.stripe_customer_email_sk
),

joined_attribution as (
    select
        joined_stripe.*,

        --first touch attribution
        user_first_touch_attribution.first_touch_utm_source,
        user_first_touch_attribution.first_touch_utm_campaign,
        user_first_touch_attribution.first_touch_utm_medium,
        user_first_touch_attribution.first_touch_utm_content,
        user_first_touch_attribution.first_touch_channel_attribution,

        --most recent touch attribution
        user_last_touch_attribution.last_touch_utm_source,
        user_last_touch_attribution.last_touch_utm_campaign,
        user_last_touch_attribution.last_touch_utm_medium,
        user_last_touch_attribution.last_touch_utm_content,
        user_last_touch_attribution.last_touch_channel_attribution
    from joined_stripe
    left join user_first_touch_attribution
        on joined_stripe.user_id = user_first_touch_attribution.user_id
    left join user_last_touch_attribution
        on joined_stripe.user_id = user_last_touch_attribution.user_id
),

user_array_agg as (
    select 
        --primary key
        user_id,

        --foreign keys
        app_user_email_sk,
        user_uuid,
        inviter_id,
        organization_id,
        manager_id,
        member_profile_id,
        care_profile_id,
        next_session_id,

        --user attributes
        language,
        roles,
        title,
        state,

        ----stripe metadata
        is_direct_to_consumer_customer,

        ---marketing attributes
        ----first touch
        first_touch_utm_source,
        first_touch_utm_campaign,
        first_touch_utm_medium,
        first_touch_utm_content,
        first_touch_channel_attribution,

        ----last touch
        last_touch_utm_source,
        last_touch_utm_campaign,
        last_touch_utm_medium,
        last_touch_utm_content,
        last_touch_channel_attribution,

        --coach related attributes
        coaching_language,

        ---lifecycle timestamps
        created_at,
        updated_at,
        confirmed_at,
        care_confirmed_at,
        lead_confirmed_at,
        confirmation_sent_at,
        current_sign_in_at,
        completed_member_onboarding_at,
        completed_account_creation_at,
        completed_primary_modality_setup_at,
        deactivated_at,
        time_zone,
        last_engaged_at,

        --other
        pending_primary_recommendation_count,
        is_call_recording_allowed,  
       --group all the possible stripe customer_id into an array to reduce records 
       --and dedup user_id

       array_agg(stripe_customer_id) as stripe_customer_id_array
    from joined_attribution
    {{ dbt_utils.group_by(n=40) }}
),

final as (
    select
        user_array_agg.*,
        array_size(stripe_customer_id_array) as count_unique_stripe_id
    from user_array_agg
)

select * from final