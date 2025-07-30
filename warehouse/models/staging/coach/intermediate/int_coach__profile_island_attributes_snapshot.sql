with profile_island_attributes_snapshot as (

    select * from {{ ref('stg_coach__profile_island_attributes_snapshot') }}

),

deleted_records AS (
    select
      item_id
    from {{ ref('stg_app_centralized__versions') }}
    where item_type = 'ProfileIslandAttribute'
    and event = 'destroy'
),

final as (

    select
        pia.*,

        --derived snapshot fields
        pia.valid_to is null as is_current_version,
        row_number() over(
          partition by pia.profile_island_attribute_uuid
          order by pia.valid_from
        ) as version,
        case when
          row_number() over(
            partition by pia.profile_island_attribute_uuid, date_trunc('day', pia.valid_from)
            order by pia.valid_from desc
          ) = 1 then true else false end as is_last_snapshot_of_day
    from profile_island_attributes_snapshot pia
    left join deleted_records dr
        on pia.profile_island_attribute_id = dr.item_id
    where dr.item_id is null
    and {{ filter_by_island(var("account_env", "")) }}  -- macro called filter_by_island.sql

)

select * from final