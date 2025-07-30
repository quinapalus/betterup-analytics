with source as (

    select * from {{ ref('base_coach__profile_island_attributes') }}

),

renamed as (

    select
        --ids
        id AS profile_island_attribute_id,
        uuid AS profile_island_attribute_uuid,
        coach_profile_uuid,

        --other
        island, --this is filtered by island in the base layer
        appointment_buffer_enabled,
        current_volunteer_member_count,
        engaged_member_count,
        max_member_count,
        organization_block_list AS banned_organization_ids,
        pending_primary_recommendation_count,
        pick_rate,
        staffable_state,

        --dates
        {{ load_timestamp('created_at') }},
        {{ load_timestamp('updated_at') }}
        
    from source

)

select * from renamed
