{{
  config(
    tags=['run_test_true']
  )
}}

with date_spine as (
-- from https://github.com/dbt-labs/dbt-utils#date_spine-source

    {{ dbt_utils.date_spine(
    datepart ="day",
    start_date = "to_date('2022-04-14', 'YYYY-MM-DD')",
    end_date="current_date+1") }}
),

base as (

    select *
    from {{ ref('int_sfdc__leads_contacts_union') }}
    qualify row_number() over (partition by sfdc_person_id, DATE_TRUNC('day', valid_from)
                               order by valid_from desc) = 1

)

    select
      {{ dbt_utils.surrogate_key(['sfdc_person_id', 'date_day']) }}  as daily_snapshot_unique_key,
      date_trunc('day', date_day)                                  as as_of_date,
      iff(as_of_date = max(date_trunc('day', date_day)) over() and is_deleted = false, TRUE, FALSE)   as is_currently_valid,
      base.*
    from base
    inner join date_spine
      on base.valid_from::date <= date_spine.date_day
      and (base.valid_to::date > date_spine.date_day or base.valid_to is null)
