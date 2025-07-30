with source as (

    select * from {{ source('coach', 'coach_growth_path_pay_rates') }}

),

renamed as (

    select
        id as coach_growth_path_pay_rate_id,
        uuid as coach_growth_path_pay_rate_uuid,
        {{ load_timestamp('created_at') }},
        {{ load_timestamp('updated_at') }},
        amount_usd,
        coaching_cloud,
        name,
        tier
    from source

)

select * from renamed

