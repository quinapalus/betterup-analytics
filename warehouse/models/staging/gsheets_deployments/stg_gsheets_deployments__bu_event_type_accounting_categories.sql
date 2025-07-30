with bu_event_type_accounting_categories as (

  select * from {{ source('gsheets_deployments', 'bu_event_type_accounting_categories') }}

)

, final as (

    select
        {{ dbt_utils.surrogate_key(['deployment_type', 'event_type', 'accounting_category']) }} as bu_event_type_accounting_category_key,
        deployment_type,
        event_type,
        accounting_category,
        parent_accounting_category
    from bu_event_type_accounting_categories

)

select * from final
