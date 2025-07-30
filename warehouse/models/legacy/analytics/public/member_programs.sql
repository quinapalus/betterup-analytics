with memcal1 as (

    select * from {{ ref('member_platform_calendar') }}

),

member_programs as (

  select * from memcal1

), 

filtered as (
select * from member_programs
 // keeping this updated to the latest date with the where clause.
 // ToDo: Macro that filters to today()
where (((date) >=
      ((date_trunc('day', convert_timezone('UTC', 'America/Los_Angeles', cast(CURRENT_TIMESTAMP() AS timestamp_ntz))))) AND
      ( date  ) <
      ((dateadd('day', 1, date_trunc('day', convert_timezone('UTC', 'America/Los_Angeles', cast(CURRENT_TIMESTAMP() AS timestamp_ntz))))))))
),

final as (
  select 
    {{dbt_utils.surrogate_key(['member_platform_calendar_id'])}} as _unique,
    *
  from filtered
)

select * from final
