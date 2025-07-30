with source as (
  select * from {{ source('hubspot', 'form_submissions') }}
),

renamed as (
    select
        --primary
        id as hubspot_form_submission_id,

        --foreign key
        portal_id as hubspot_portal_id,
        page_id as hubspot_page_id,
        form_id as hubspot_form_id,

        --attributes
        form_type,
        content_type,
        title,
        page_title,
        page_url,
        canonical_url,
        contact_associated_by_0,

        ----timestmaps
        uuid_ts,
        timestamp,
        received_at

    from source
)

select * from renamed
