{%- call statement('get_max_seats', fetch_result=True) -%}
      SELECT coalesce(max(seats),0) as max_seats FROM {{ref('stg_app__contract_line_items')}}
{%- endcall -%}

{%- set max_seats = load_result('get_max_seats')['data'][0][0] -%}

with dim_date as (
    select * from  {{ ref('dim_date') }}
),

contract_line_items as (
    select * from  {{ ref('stg_app__contract_line_items') }}
),

products as (
  select * from {{ref('dim_products')}}
),

cpq2_contract_line_items as (

    select cli.* 
    from contract_line_items as cli
    inner join products as p
        on cli.product_id = p.product_id
    where p.source = 'CPQ2'

),

contract_line_items_expanded as (
    
    select 
        cpq2_contract_line_items.*,
        row_number() over(partition by organization_id, product_id, product_subscription_id order by starts_at, contract_line_item_id) as rn
    from cpq2_contract_line_items
    -- generates a row for every seat of the CLI using the number of seats from contract line items (contract_line_items.seats)
    inner join (select row_number() over(order by seq4()) as r from table(generator(rowcount => {{max_seats}}))) AS v on v.r <= seats

),

joined as (

    select
        c.date_key,
        c.date,
        c.calendar_year_month,
        c.is_current_fiscal_quarter,
        c.is_previous_fiscal_quarter,
        c.date = last_day(c.date) as is_last_day_of_month,
        cli.starts_at,
        date_trunc('day', cli.starts_at) as starts_date,
        date_trunc('day', cli.ends_at) as ends_date,
        cli.contract_line_item_id,
        cli.organization_id,
        cli.product_id,
        cli.product_subscription_id,
        cli.coaching_months,
        cli.deployment_type,
        cli.rn
    from dim_date as c
    inner join contract_line_items_expanded as cli
        on 
            c.date >= starts_date and
            c.date <= ends_date
            
),

final as (
    
    select 
        {{ dbt_utils.surrogate_key(['contract_line_item_id', 'date_key', 'rn']) }} as contract_line_item_calendar_id,
        *,
        row_number() over(partition by date, organization_id, product_id, product_subscription_id order by starts_at, contract_line_item_id) as seat_number
    from joined

)

select * from final
