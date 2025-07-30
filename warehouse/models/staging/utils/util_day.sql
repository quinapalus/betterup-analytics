with days as (

{{ dbt_utils.date_spine(
    datepart="day",
    start_date="to_date('2013-01-01', 'YYYY-MM-DD')",
    end_date="dateadd('day', 1, date(sysdate()))"
   )
}}

),

renamed as (
    select 
        --primary key
        {{ dbt_utils.surrogate_key(['date_day'])}} as _unique,

        --attributes
        date_day,
        date_trunc('month', date_day) as date_month,
        last_day(date_day) as last_day_of_the_month,
        iff(date_day = last_day_of_the_month, true, false) as is_last_day_of_the_month
    from days
)

select * from renamed