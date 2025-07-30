with source as (

    select * from {{ source('app', 'care_topic_selections') }}

),

renamed as (

    select
        id as care_topic_selection_id,
        care_profile_id,
        care_topic_id,
        {{ load_timestamp('created_at') }},
        {{ load_timestamp('updated_at') }}

    from source

)

select * from renamed