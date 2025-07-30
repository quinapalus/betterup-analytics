with dim_date as (
  select * from {{ ref('dim_date') }}
),

product_subscription_assignments as (
   select * from {{ ref('stg_app__product_subscription_assignments') }}
),

product_subscriptions as (
  select * from {{ref('dim_product_subscriptions')}}
),

products as (
  select * from {{ref('dim_products')}}
),

join_products as (
  select
      psa.product_subscription_assignment_id,
      psa.member_id,
      psa.starts_at,
      psa.ends_at,
      psa.ended_at,
      ps.product_subscription_id,
      ps.organization_id,
      ps.non_transfer_period,
      p.product_id
  from product_subscription_assignments as psa
  inner join product_subscriptions as ps
    on psa.product_subscription_id = ps.product_subscription_id
  inner join products as p
    on ps.product_id = p.product_id
  where 
    psa.v2 and 
    p.source = 'CPQ2'
),

psas as (
  select
    c.date_key,
    c.date,
    c.calendar_year_month,
    c.is_current_fiscal_quarter,
    c.is_previous_fiscal_quarter,
    c.date = last_day(c.date) as is_last_day_of_month,
    date_trunc('day', p.starts_at) as starts_date,
    date_trunc('day', coalesce(p.ended_at, p.ends_at)) as ends_date,
    p.*
  from dim_date as c
  inner join join_products as p
    on 
      c.date >= starts_date and
      c.date <= ends_date
),

final as (
  select
    {{ dbt_utils.surrogate_key(['product_subscription_assignment_id', 'date_key']) }} as product_subscription_assignment_calendar_id,
    *,
    row_number() over(partition by date, organization_id, product_id, product_subscription_id order by starts_at, product_subscription_assignment_id) as psa_rownum
  from psas
)

select * from final