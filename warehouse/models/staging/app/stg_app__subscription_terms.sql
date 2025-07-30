{{
  config(
    tags=['eu']
  )
}}

with source as (
    select * from {{ source('app', 'subscription_terms') }}
),
destroyed_records as (
    select * from {{ ref('stg_app__versions_delete')}}
    where item_type = 'SubscriptionTerms'
),
filtered as (
    select s.*
    from source s
    left join destroyed_records as v 
        on s.id = v.item_id
    where v.item_id is null
),
renamed as (

    select
        --Primary Key
        id as subscription_terms_id,

        --Logical data
        care_limit,
        care_limit_cadence,
        coaching_circles_limit,
        coaching_circles_limit_cadence,
        exact_specialist_verticals,
        non_transfer_period,
        on_demand_limit,
        on_demand_limit_cadence,
        primary_coaching_limit,
        primary_coaching_limit_cadence,
        specialist_coaching_limit,
        specialist_coaching_limit_cadence,
        transferable,
        
        --Timestamps
        {{ load_timestamp('created_at') }},
        {{ load_timestamp('updated_at') }}

    from filtered

)

select * from renamed
