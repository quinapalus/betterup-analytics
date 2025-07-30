with lead_metadata_attributes as (
    --contains event log of attribution events for users
    select * from {{ ref('stg_app__lead_metadata_attributes')}}
),

google_sheet_attribution_mapping as (
    select * from {{ ref('stg_gsheets_marketing_attribution_sk_mapping__metadata_attribution')}}
),

user_attribution_log as (
    select 
        lead_metadata_attributes.*,
        google_sheet_attribution_mapping.channel_attribution as channel_attribution
    from lead_metadata_attributes
    left join google_sheet_attribution_mapping
        on lead_metadata_attributes.utm_source = google_sheet_attribution_mapping.utm_source
        and lead_metadata_attributes.utm_campaign = google_sheet_attribution_mapping.utm_campaign
        and lead_metadata_attributes.utm_medium = google_sheet_attribution_mapping.utm_medium
        and lead_metadata_attributes.utm_content = google_sheet_attribution_mapping.utm_content
),

final as (
    select

        --primary key
        lead_metadata_attribute_id,
    
        --foreign keys
        user_id,
        invitation_id,   

        --attribution related 
        utm_source,
        utm_campaign,
        utm_medium,
        utm_content,
        channel_attribution,
        attribution_order,
        reverse_attribution_order,

        --timestamps
        created_at,
        updated_at
    from user_attribution_log
)

--this intermediate table is set at the core layer because it joins data from 2 different sources (App, Google Sheets)
--it is useful to separate so more unit tests can be applied to this model.
select * from final