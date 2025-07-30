with source as (
  
    select * from {{ source('heap', 'pageviews') }}
    
),

deduplicate as (
    
    select 
    
        *,
        row_number() over (partition by event_id order by time) as dedupe_id
        
    from source

)

--there are many instances of the event_id being duplicated. further 
--assessment needed but this will deduplicate the event by 
--taking the first instance of the event_id for downstream models

select * from deduplicate
where dedupe_id =1