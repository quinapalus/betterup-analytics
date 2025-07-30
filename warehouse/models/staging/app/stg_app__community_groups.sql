with source as (

    select * from {{ source('app', 'community_groups') }}

),

renamed as (

    select
        id as community_group_id,
        description,
        name,
        posts_count,
        {{ load_timestamp('created_at') }},
        {{ load_timestamp('updated_at') }}

    from source

)

select * from renamed