with product_subscription_assignment_calendar as (
    select * from {{ ref('product_subscription_assignment_calendar') }}
),

product_subscription_assignments as (
    select * from  {{ ref('dim_product_subscription_assignments') }}
),

contract_line_item_calendar as (
    select * from {{ ref('contract_line_item_calendar') }}
),

contract_line_items as (
    select *, min(starts_at) over(partition by organization_id, product_id, product_subscription_id) as min_cli_starts_at
    from {{ ref('stg_app__contract_line_items') }}
),

contracts as (
    select * from {{ ref('stg_app__contracts') }}
),

product_subscription_assignment_consumption as (
    select * from  {{ ref('product_subscription_assignment_consumption') }}
),

currently_active_cli AS (
    select 
        contract_line_item_id, 
        cli.organization_id, 
        product_id,
        product_subscription_id,
        min_cli_starts_at,
        row_number() over(partition by c.organization_id, cli.product_id, cli.product_subscription_id order by cli.starts_at desc) as r
     from contract_line_items as cli
     inner join contracts as c
        on c.contract_id = cli.contract_id
     where current_date between cli.starts_at and cli.ends_at
     qualify r = 1 
),

joined as (

    select
        p.date as utilization_date,
        c.date as availability_date,
        coalesce(p.date, c.date) as calendar_date,
        coalesce(c.contract_line_item_id, ccli.contract_line_item_id) as contract_line_item_id,
        coalesce(p.organization_id, c.organization_id) as organization_id,
        coalesce(p.psa_rownum, c.seat_number) as seat_number,
        p.member_id,
        p.product_subscription_assignment_id,
        p.non_transfer_period,
        coalesce(p.product_subscription_id, c.product_subscription_id) as product_subscription_id, 
        coalesce(p.product_id, c.product_id) as product_id,
        datediff(month, c.starts_date, dateadd(day, 1, c.ends_date)) as coaching_term,
        c.coaching_months, 
        iff(c.contract_line_item_id is null, True, False) as is_seat_over_assigned
    from product_subscription_assignment_calendar as p
    full outer join contract_line_item_calendar as c
        on 
            p.organization_id = c.organization_id and
            p.product_id = c.product_id and 
            p.product_subscription_id = c.product_subscription_id and 
            p.date_key = c.date_key and 
            p.psa_rownum = c.seat_number 
    -- Attempt to join over-assigned PSAs to currently active CLIs  
    left outer join currently_active_cli as ccli
        on
            p.organization_id = ccli.organization_id and 
            p.product_id = ccli.product_id and 
            p.product_subscription_id = ccli.product_subscription_id and 
            c.seat_number IS NULL and 
            p.starts_at >= ccli.min_cli_starts_at

),

-- The logic in the next 4 CTEs will mark a seat as assigned if a seat was fully utilizaed in the past,
-- even if no member is currently occupying the seat
join_psa_consumption as (

    select 
        c.*, 
        pc.unassignment_reason, 
        pc.unassigned_at  
    from joined as c
    left join product_subscription_assignments as psa
      on c.product_subscription_assignment_id = psa.product_subscription_assignment_id
    left join contract_line_items as cli
      on c.contract_line_item_id = cli.contract_line_item_id
    left join product_subscription_assignment_consumption as pc
        on 
            c.product_subscription_assignment_id = pc.product_subscription_assignment_id and 
            psa.starts_at < cli.ends_at and 
            coalesce(psa.ended_at, psa.ends_at) > cli.starts_at

),

get_min_utilization_date as (

    select distinct 
        contract_line_item_id,
        seat_number,
        min(utilization_date) as min_utilization_date
    from join_psa_consumption
    where 
      unassignment_reason = 'ended' and 
      coaching_term != coaching_months
    group by contract_line_item_id, seat_number

),

join_min_utilization_date as (

    select distinct 
        c.contract_line_item_id,
        c.seat_number,
        c.unassignment_reason,
        mu.min_utilization_date
    from join_psa_consumption as c
    left join get_min_utilization_date as mu
        on 
            c.contract_line_item_id = mu.contract_line_item_id and 
            c.seat_number = mu.seat_number

),

count_fully_assignmed_seats as (

    select 
        contract_line_item_id,
        seat_number,
        min_utilization_date,
        count_if(unassignment_reason = 'ended') as seat_fully_assigned_count
    from join_min_utilization_date
    group by contract_line_item_id, seat_number, min_utilization_date

),

final as (
    
    select 
        {{ dbt_utils.surrogate_key(['c.product_subscription_assignment_id', 'c.contract_line_item_id', 'c.calendar_date', 'c.seat_number']) }} as contract_utilization_calendar_id,
        c.*,
        case 
          when c.product_subscription_assignment_id is not null then true
          when cs.seat_fully_assigned_count >= 1 then true
          else false
        end as is_seat_assigned        
    from join_psa_consumption as c
    left join count_fully_assignmed_seats as cs
        on 
            c.contract_line_item_id = cs.contract_line_item_id and 
            c.seat_number = cs.seat_number and 
            c.calendar_date >= cs.min_utilization_date

)

select * from final
