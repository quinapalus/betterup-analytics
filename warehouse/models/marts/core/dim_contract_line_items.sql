{{
  config(
    tags=['eu']
  )
}}

with contract_line_items as (
  select * from {{ ref('stg_app__contract_line_items') }}
),

final as (
  select 
    *,
    iff(current_date() between starts_at and coalesce(ends_at, current_date() + 1), true, false) as is_active,
    case
      when current_date() between starts_at and coalesce(ends_at, current_date() + 1) then 'Active'
      when current_date() < starts_at then 'Upcoming'
      when current_date() > ends_at then 'Ended'
      else null
    end as status
  from contract_line_items as cli
)

select * from final