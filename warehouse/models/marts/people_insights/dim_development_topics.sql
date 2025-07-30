with development_topics as (

    select * from {{ ref('stg_app__development_topics') }}

)

select * from development_topics
