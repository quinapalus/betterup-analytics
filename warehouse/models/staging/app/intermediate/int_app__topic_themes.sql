with topic_themes as (
    select * from {{ ref('stg_app__topic_themes') }}
)

select * from topic_themes
