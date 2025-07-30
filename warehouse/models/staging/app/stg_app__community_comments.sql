with source as (

    select * from {{ source('app', 'community_comments') }}

),

renamed as (

    select
        id as community_comment_id,
        user_id,
        post_id,
        parent_comment_id,
        body,
        nested_level,
        child_comments_count,
        {{ load_timestamp('deleted_at') }},
        {{ load_timestamp('published_at') }},
        {{ load_timestamp('last_edited_at') }},
        {{ load_timestamp('created_at') }},
        {{ load_timestamp('updated_at') }}


    from source

)

select * from renamed
