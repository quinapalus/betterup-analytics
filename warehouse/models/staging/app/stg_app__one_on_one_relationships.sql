with source as (

    select * from {{ source('app', 'one_on_one_relationships') }}

),

renamed as (

    select
        id as one_on_one_relationship_id,
        first_participant_id,
        second_participant_id,
        in_progress_one_on_one_topics_count,
        {{ load_timestamp('created_at') }},
        {{ load_timestamp('updated_at') }}

    from source

)

select * from renamed