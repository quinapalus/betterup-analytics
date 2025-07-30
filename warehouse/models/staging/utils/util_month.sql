with months as (

{{ dbt_utils.date_spine(
    datepart="month",
    start_date="to_date('2013-01-01', 'YYYY-MM-DD')",
    end_date="dateadd('month', 1, date(sysdate()))"
   )
}}

),

renamed as (
    select 
        --primary key
        {{ dbt_utils.surrogate_key(['date_month'])}} as _unique,

        --attributes
        date_month,
        dateadd('month', 1, date_month) as next_month,
        dateadd('day', -1, next_month) as last_month_day
    from months
)

select * from renamed