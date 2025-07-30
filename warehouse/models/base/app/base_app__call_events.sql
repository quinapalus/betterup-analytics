with call_events as (

    select * from {{ source('p2pc', 'call_events') }}

)

--adding distinct as an intermim solution for some duplicate UUIDs. engineering is working on a fix upstream
select distinct *
from call_events
where call_events.source = 'production'
