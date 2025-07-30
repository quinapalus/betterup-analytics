--refactoring dim_users (to be deprecated once Looker explores are pointed here)

with users as (
  select * from {{ ref('int_app__users') }}
),

user_attribution_log as (
    select * from {{ ref('int_user_attribution_log') }}
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

final as (
    select
        users.*,

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
    from users
    left join user_first_touch_attribution
        on users.user_id = user_first_touch_attribution.user_id
    left join user_last_touch_attribution
        on users.user_id = user_last_touch_attribution.user_id
)

select * from final