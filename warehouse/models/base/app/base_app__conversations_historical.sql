with archived_conversations as (

    select * from {{ source('app_archive', 'conversations') }}

)

select * from archived_conversations