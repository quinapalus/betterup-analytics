{{
  config(
    tags=['eu']
  )
}}

with product_subscriptions as (
    select * from {{ref('stg_app__product_subscriptions')}}
),
subscription_terms as (
    select * from {{ref('stg_app__subscription_terms')}}
),
joined as (

    select 
        --Primary Key
        product_subscription_id,

        --Foreign Keys
        ps.product_id,
        ps.organization_id,
        ps.subscription_terms_id,

        --Logical data
        ps.state,
        ps.name,
        s.care_limit,
        s.care_limit_cadence,
        s.coaching_circles_limit,
        s.coaching_circles_limit_cadence,
        s.exact_specialist_verticals,
        s.non_transfer_period,
        s.on_demand_limit,
        s.on_demand_limit_cadence,
        s.primary_coaching_limit,
        s.primary_coaching_limit_cadence,
        s.specialist_coaching_limit,
        s.specialist_coaching_limit_cadence,
        s.transferable,

        --Timestamps
        ps.{{ load_timestamp('created_at') }},
        ps.{{ load_timestamp('updated_at') }}
    from product_subscriptions as ps
    left join subscription_terms as s
        on ps.subscription_terms_id = s.subscription_terms_id

)

select * from joined
