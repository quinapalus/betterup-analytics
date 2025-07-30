with source as (

    select * from {{ source('app', 'community_posts') }}

),

renamed as (

    select
        id as community_post_id,
        group_id,
        user_id,
        body,
        comments_count,
        image_status,
        {{ load_timestamp('created_at') }},
        {{ load_timestamp('deleted_at') }},
        {{ load_timestamp('published_at') }},
        {{ load_timestamp('last_edited_at') }},
        {{ load_timestamp('updated_at') }}


    from source

)

select * from renamed
