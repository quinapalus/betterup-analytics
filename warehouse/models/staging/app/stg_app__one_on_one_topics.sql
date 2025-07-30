with source as (

    select * from {{ source('app', 'one_on_one_topics') }}

),

renamed as (

    select
        id as one_on_one_topic_id,
        creator_id,
        relationship_id,
        state,
        title,
        notes_count,
        {{ load_timestamp('state_updated_at') }},
        {{ load_timestamp('created_at') }},
        {{ load_timestamp('updated_at') }}

    from source

)

select * from renamed