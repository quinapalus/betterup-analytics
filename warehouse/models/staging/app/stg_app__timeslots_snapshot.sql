with timeslots_snapshot as (

    select * from {{ ref('snapshot_coach__timeslots') }}

),

final as (
    select
        --ids
        id as timeslot_id,
        {{ dbt_utils.surrogate_key(['id','dbt_valid_from','dbt_valid_to']) }} as history_primary_key,
        user_id as coach_id,

        --other
        available_minutes,
        external_unavailable_minutes,

        --dates
        starts_at,
        ends_at,
        created_at,
        updated_at,

        --snapshot fields
        dbt_valid_from as valid_from,
        dbt_valid_to as valid_to

    from timeslots_snapshot

)

select * from final