with sendgrid_events_source as (
    
     select * from {{ source('sendgrid', 'events') }}
     
),

sendgrid_events_renamed as (
    
    select

        --ids
        {{ dbt_utils.surrogate_key(['"sg_event_id"','"timestamp"'])}} as sendgrid_event_id,
        "user_id" as user_id,    
        "asm_group_id" as asm_group_id,
        "sg_event_id" as sg_event_id,
        "sg_message_id" as sg_message_id,
        "smtp-id" as smtp_id,
        
        --event information
        "attempt" as attempt,
        "category" as category,
        "email" as email,
        "event" as event,
        "ip" as ip,
        "reason" as reason,
        "response" as response,
        "status" as status,
        "url" as url,
        "useragent" as useragent,
        "tls" as tls,
        "url_offset__index" as url_offset__index,
        "url_offset__type" as url_offset__type,
        "cert_err" as cert_err,
        "type" as event_type,
        "sg_content_type" as sg_content_type,
                
        --timestamps
        "date" as event_date,
        "timestamp" as event_at,
        "processed" as processed_at,
        "send_at" as send_at
        
    from sendgrid_events_source
    
    /* The column names row piped in with the raw table. 
    This will remove it. */
    where event_at != 'timestamp'
    
)

select * from sendgrid_events_renamed