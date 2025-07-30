-- More information on CLI transferability can be found here - https://betterup.atlassian.net/wiki/spaces/LM/pages/3353313281/Assignment+in+Lead+Reporting+Short-term+solution+for+transferable+vs.+non-transferable+deals
with product_subscription_assignments as (
    select * from {{ ref('stg_app__product_subscription_assignments') }}
),

product_subscriptions as (
    select * from {{ref('dim_product_subscriptions')}}
),

users as (
    select * from {{ ref('stg_app__users') }}
),

joined as (

    select 
        psa.product_subscription_assignment_id,
        u.last_engaged_at,
        psa.starts_at,
        datediff('day', psa.ended_at, psa.ends_at) as end_diff,
        case
            when 
                ps.non_transfer_period = 12 and 
                u.last_engaged_at < least(coalesce(psa.ended_at, psa.ends_at)) and current_date() > dateadd('day', 60, u.last_engaged_at) and (end_diff >= 30 or psa.ended_at is null) then 'inactive'
            when
                ps.non_transfer_period != 0 and 
                end_diff > 30 then 'ended early'
            else 'ended'
        end as unassignment_reason,
        case 
            when unassignment_reason = 'inactive' then datediff('day', least(dateadd('day', 60, u.last_engaged_at), coalesce(psa.ended_at, psa.ends_at)), psa.ends_at)
            when unassignment_reason = 'ended early' then end_diff
            else null
        end as partial_consumption_leftover_period,
        case 
            when unassignment_reason = 'inactive' then least(dateadd('day', 60, u.last_engaged_at), coalesce(psa.ended_at, psa.ends_at))
            when unassignment_reason = 'ended early' then psa.ended_at
            when (ps.non_transfer_period = 0 or ps.non_transfer_period is null) then coalesce(psa.ended_at, psa.ends_at)
            else psa.ends_at
        end as unassigned_at 
    from product_subscription_assignments as psa
    left join product_subscriptions as ps
        on psa.product_subscription_id = ps.product_subscription_id
    left join users as u
        on psa.member_id = u.user_id

),

final as (
    
    select 
        {{ dbt_utils.surrogate_key(['product_subscription_assignment_id']) }} as product_subscription_assignment_consumption_id,
        *
    from joined

)

select * from final
