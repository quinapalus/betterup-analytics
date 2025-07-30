with archived_member_development_topics as (

    select * from {{ source('app_archive', 'member_development_topics') }}

)

select * from archived_member_development_topics