with source as (

    select * from {{ source('app', 'care_topics') }}

),

renamed as (

    select
        id as care_topic_id,
        program_journey_id,
        growth_focus_area_id,
        {{ sanitize_i18n_field('description') }}
        {{ sanitize_i18n_field('name') }}
        translation_key,
        uuid,
        {{ load_timestamp('created_at') }},
        {{ load_timestamp('updated_at') }}

    from source

)

select * from renamed