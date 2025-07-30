with archived_conversation_participants as (

    select * from {{ source('app_archive', 'conversation_participants') }}

)

select * from archived_conversation_participants